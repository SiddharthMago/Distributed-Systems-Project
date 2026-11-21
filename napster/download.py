"""Bounded downloads, durable checkpoints, and application payload accounting."""

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import time
from contextlib import closing, contextmanager
from pathlib import Path
from uuid import uuid4

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector

from .model import BLOCK, Manifest, durable_replace, sha256_file
from .service import connect

LOG = logging.getLogger(__name__)
SCHEMA = """
CREATE TABLE IF NOT EXISTS job (id TEXT PRIMARY KEY, manifest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chunks (
    idx INTEGER PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
    complete INTEGER NOT NULL DEFAULT 0, commits INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS attempts (
    seq INTEGER PRIMARY KEY, idx INTEGER REFERENCES chunks(idx), peer TEXT NOT NULL,
    bytes INTEGER NOT NULL, repeated INTEGER NOT NULL, outcome TEXT NOT NULL
);
"""


class TransferError(Exception):
    pass


def state_dir(output):
    output = output.resolve()
    return output.with_name(output.name + ".napster")


@contextmanager
def exclusive_job(state):
    state.mkdir(parents=True, exist_ok=True)
    with (state / "lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TransferError("A downloader already holds this output checkpoint") from error
        yield


class Checkpoint:
    def __init__(self, state, db, ident, manifest):
        self.state = state
        self.db = db
        self.ident = ident
        self.manifest = manifest
        row = db.execute("SELECT id, manifest FROM job").fetchone()
        if row and row[0] != ident:
            raise TransferError("This output has a checkpoint for a different file")
        db.execute("BEGIN")
        try:
            if row is None:
                db.execute("INSERT INTO job VALUES (?, ?)", (ident, json.dumps(manifest.to_dict())))
                db.executemany("INSERT INTO chunks(idx, request_id) VALUES (?, ?)",
                               ((index, str(uuid4())) for index in range(len(manifest.chunks))))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def path(self, index):
        return self.state / f"{index:08d}.chunk"

    def commit(self, index):
        self.db.execute(
            "UPDATE chunks SET complete=1, commits=commits+1 WHERE idx=? AND complete=0", (index,)
        )

    def reconcile(self):
        missing = []
        for index, request_id, complete in self.db.execute(
            "SELECT idx, request_id, complete FROM chunks ORDER BY idx"
        ).fetchall():
            path = self.path(index)
            path.with_suffix(".tmp").unlink(missing_ok=True)
            if (path.exists() and path.stat().st_size == self.manifest.length(index)
                    and sha256_file(path) == self.manifest.chunks[index]):
                if not complete:
                    self.commit(index)
                LOG.info("reuse chunk=%d request_id=%s", index, request_id)
            else:
                if complete or path.exists():
                    LOG.warning("invalidate saved chunk=%d request_id=%s", index, request_id)
                self.db.execute("UPDATE chunks SET complete=0 WHERE idx=?", (index,))
                path.unlink(missing_ok=True)
                missing.append((index, request_id))
        return missing

    def record_attempt(self, index, peer, received, outcome):
        self.db.execute("BEGIN")
        try:
            prior = self.db.execute(
                "SELECT COALESCE(MAX(bytes), 0) FROM attempts WHERE idx=?", (index,)
            ).fetchone()[0]
            self.db.execute(
                "INSERT INTO attempts(idx, peer, bytes, repeated, outcome) VALUES (?, ?, ?, ?, ?)",
                (index, peer, received, min(prior, received), outcome),
            )
            if outcome == "verified":
                self.commit(index)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise


def status(output, details=False):
    path = state_dir(output) / "checkpoint.sqlite3"
    if not path.is_file():
        raise TransferError("No checkpoint for this output")
    with closing(connect(path)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        row = db.execute("SELECT id, manifest FROM job").fetchone()
        if row is None:
            raise TransferError("Checkpoint initialization is incomplete")
        manifest = Manifest.parse(json.loads(row["manifest"]))
        total, complete, commits = db.execute(
            "SELECT count(*), COALESCE(sum(complete), 0), COALESCE(sum(commits), 0) FROM chunks"
        ).fetchone()
        payload, repeated = db.execute(
            "SELECT COALESCE(sum(bytes), 0), COALESCE(sum(repeated), 0) FROM attempts"
        ).fetchone()
        peers = [dict(item) for item in db.execute("""
            SELECT peer, count(*) AS attempts, sum(bytes) AS payload_bytes,
                sum(outcome='verified') AS verified_chunks
            FROM attempts GROUP BY peer ORDER BY peer
        """)]
        result = {
            "file_id": row["id"], "expected_sha256": manifest.sha256,
            "total_chunks": total, "verified_chunks": complete, "commits": commits,
            "payload_bytes": payload, "retransmitted_bytes": repeated, "peers": peers,
        }
        if details:
            result["chunks"] = [dict(item) for item in db.execute("SELECT * FROM chunks ORDER BY idx")]
            result["attempts"] = [dict(item) for item in db.execute("SELECT * FROM attempts ORDER BY seq")]
        return result


async def lookup(session, directory, ident):
    async with session.get(directory + "/files/" + ident) as response:
        response.raise_for_status()
        record = await response.json()
    manifest = Manifest.parse(record["manifest"])
    if manifest.id != ident:
        raise TransferError("Directory returned a manifest with the wrong ID")
    return manifest, record["sources"]


async def fetch(session, checkpoint, index, request_id, source):
    manifest = checkpoint.manifest
    expected = manifest.chunks[index]
    length = manifest.length(index)
    target = checkpoint.path(index)
    temp = target.with_suffix(".tmp")
    peer = source["peer"]
    url = f"{source['url']}/chunks/{checkpoint.ident}/{index}"
    headers = {"X-Request-ID": request_id, "X-Chunk-SHA256": expected}
    digest = hashlib.sha256()
    received = 0
    outcome = "cancelled"
    try:
        with temp.open("wb") as stream:
            async with session.get(url, headers=headers, allow_redirects=False) as response:
                response.raise_for_status()
                if (response.status != 200 or response.content_length != length
                        or response.headers.get("X-Request-ID") != request_id
                        or response.headers.get("X-Chunk-SHA256") != expected):
                    raise TransferError("Invalid chunk response metadata")
                async for block in response.content.iter_chunked(BLOCK):
                    received += len(block)
                    stream.write(block)
                    digest.update(block)
            if received != length or digest.hexdigest() != expected:
                raise TransferError("Chunk length or SHA-256 mismatch")
            stream.flush()
            os.fsync(stream.fileno())
        durable_replace(temp, target)
        outcome = "verified"
    except (ClientError, TimeoutError, TransferError) as error:
        outcome = type(error).__name__
        LOG.warning("retry chunk=%d request_id=%s peer=%s received=%d error=%s",
                    index, request_id, peer, received, error)
        return False, isinstance(error, (ClientError, TimeoutError))
    except OSError:
        outcome = "local_io_error"
        raise
    finally:
        checkpoint.record_attempt(index, peer, received, outcome)
        temp.unlink(missing_ok=True)
    LOG.info("commit chunk=%d request_id=%s peer=%s", index, request_id, peer)
    return True, False


async def transfer(session, directory, checkpoint, missing, sources, workers, rounds):
    queue = iter(missing)
    unavailable_until = {}
    failures = []
    ident = checkpoint.ident

    async def worker():
        for index, request_id in queue:
            options = sources
            for turn in range(rounds):
                if turn:
                    await asyncio.sleep(min(0.25 * 2 ** (turn - 1), 2))
                    try:
                        _, options = await lookup(session, directory, ident)
                    except (ClientError, TimeoutError):
                        LOG.warning("Directory unavailable during source refresh")
                if not options:
                    continue
                start = index % len(options)
                ordered = options[start:] + options[:start]
                ordered.sort(key=lambda source: unavailable_until.get(source["url"], 0) > time.monotonic())
                for source in ordered:
                    success, transport_failed = await fetch(session, checkpoint, index, request_id, source)
                    if success:
                        break
                    if transport_failed:
                        unavailable_until[source["url"]] = time.monotonic() + 30
                else:
                    continue
                break
            else:
                failures.append(index)
                return

    tasks = [asyncio.create_task(worker()) for _ in range(min(workers, len(missing)))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    if failures:
        raise TransferError(f"Sources exhausted for chunks {sorted(failures)}. Run download again to resume.")


def assemble(checkpoint, output):
    manifest = checkpoint.manifest
    temp = checkpoint.state / "assembled.tmp"
    digest = hashlib.sha256()
    size = 0
    try:
        with temp.open("wb") as stream:
            for index in range(len(manifest.chunks)):
                with checkpoint.path(index).open("rb") as chunk:
                    while block := chunk.read(BLOCK):
                        digest.update(block)
                        size += len(block)
                        stream.write(block)
            actual = digest.hexdigest()
            if size != manifest.size or actual != manifest.sha256:
                raise TransferError("Reconstructed file failed length or SHA-256 verification")
            stream.flush()
            os.fsync(stream.fileno())
        durable_replace(temp, output)
        return actual
    finally:
        temp.unlink(missing_ok=True)


async def download(directory, ident, output: Path, workers=4, timeout=10, rounds=3):
    started = time.monotonic()
    output = output.resolve()
    state = state_dir(output)
    with exclusive_job(state), closing(connect(state / "checkpoint.sqlite3", SCHEMA)) as db:
        async with ClientSession(connector=TCPConnector(limit=workers),
                                 timeout=ClientTimeout(total=timeout), auto_decompress=False) as session:
            manifest, sources = await lookup(session, directory, ident)
            checkpoint = Checkpoint(state, db, ident, manifest)
            missing = checkpoint.reconcile()
            await transfer(session, directory, checkpoint, missing, sources, workers, rounds)
        actual = assemble(checkpoint, output)
        elapsed = time.monotonic() - started
        result = status(output)
        result.update({"elapsed_seconds": elapsed, "resumed_chunks": len(manifest.chunks) - len(missing),
                       "actual_sha256": actual, "hash_equal": actual == manifest.sha256})
        return result
