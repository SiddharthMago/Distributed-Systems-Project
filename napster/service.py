"""Persistent metadata directory and direct chunk servers."""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from uuid import UUID

from aiohttp import ClientSession, ClientTimeout, web
from yarl import URL

from .model import BLOCK, Manifest

LOG = logging.getLogger(__name__)
LIMIT = 32 * 1024 * 1024

DIRECTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS peers (id TEXT PRIMARY KEY, url TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, manifest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS publications (
    peer TEXT REFERENCES peers(id), file TEXT REFERENCES files(id), name TEXT NOT NULL,
    PRIMARY KEY(peer, file, name)
);
CREATE INDEX IF NOT EXISTS publications_file ON publications(file);
"""
PEER_SCHEMA = """
CREATE TABLE IF NOT EXISTS shares (
    id TEXT PRIMARY KEY, path TEXT NOT NULL, manifest TEXT NOT NULL
);
"""


def connect(path, schema=None):
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute("PRAGMA foreign_keys = ON")
        if schema:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(schema)
        db.execute("PRAGMA synchronous = FULL")
        return db
    except BaseException:
        db.close()
        raise


def init_shares(state):
    state.mkdir(parents=True, exist_ok=True)
    connect(state / "shares.sqlite3", PEER_SCHEMA).close()


async def health(request):
    return web.json_response({"status": "ok"})


@web.middleware
async def bad_input(request, handler):
    try:
        return await handler(request)
    except (ValueError, KeyError, TypeError) as error:
        raise web.HTTPBadRequest(text=str(error)) from error


def directory_app(path):
    async def database(app):
        path.parent.mkdir(parents=True, exist_ok=True)
        db = connect(path, DIRECTORY_SCHEMA)
        try:
            app[database_key] = db
            yield
        finally:
            db.close()

    database_key = web.AppKey("directory database", sqlite3.Connection)

    async def register(request):
        body = await request.json()
        peer, url = body["peer"], URL(body["url"])
        if not isinstance(peer, str) or not 1 <= len(peer) <= 128:
            raise ValueError("Invalid peer ID")
        if (url.scheme not in ("http", "https") or not url.host or url.path != "/"
                or url.query_string or url.fragment or url.user or url.password):
            raise ValueError("Peer URL must be an HTTP origin")
        request.app[database_key].execute(
            "INSERT INTO peers VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET url=excluded.url",
            (peer, str(url.origin())),
        )
        return web.json_response({"peer": peer})

    async def publish(request):
        body = await request.json()
        manifest = Manifest.parse(body["manifest"])
        peer, name = body["peer"], body["name"]
        if not isinstance(name, str) or not 1 <= len(name) <= 255:
            raise ValueError("Invalid file name")
        db = request.app[database_key]
        if not db.execute("SELECT 1 FROM peers WHERE id=?", (peer,)).fetchone():
            raise web.HTTPNotFound(text="Register peer before publishing")
        db.execute("BEGIN")
        try:
            db.execute("INSERT OR IGNORE INTO files VALUES (?, ?)",
                       (manifest.id, json.dumps(manifest.to_dict())))
            db.execute("INSERT OR IGNORE INTO publications VALUES (?, ?, ?)",
                       (peer, manifest.id, name))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        return web.json_response({"file_id": manifest.id})

    async def search(request):
        query = request.query.get("q", "").casefold()
        db = request.app[database_key]
        results = []
        for ident, name, count in db.execute(
            "SELECT file, name, count(*) FROM publications GROUP BY file, name ORDER BY name, file"
        ):
            if query in name.casefold() or query in ident:
                results.append({"file_id": ident, "name": name, "sources": count})
        return web.json_response(results)

    async def lookup(request):
        ident = request.match_info["file_id"]
        db = request.app[database_key]
        row = db.execute("SELECT manifest FROM files WHERE id=?", (ident,)).fetchone()
        if row is None:
            raise web.HTTPNotFound(text="Unknown file")
        sources = db.execute("""
            SELECT DISTINCT peers.id, peers.url FROM peers JOIN publications
            ON peers.id = publications.peer WHERE publications.file=? ORDER BY peers.id
        """, (ident,)).fetchall()
        return web.json_response({
            "manifest": json.loads(row[0]),
            "sources": [{"peer": peer, "url": url} for peer, url in sources],
        })

    app = web.Application(client_max_size=LIMIT, middlewares=[bad_input])
    app.cleanup_ctx.append(database)
    app.add_routes([
        web.get("/health", health), web.put("/peers", register),
        web.post("/publications", publish), web.get("/search", search),
        web.get("/files/{file_id}", lookup),
    ])
    return app


def peer_app(state, rate_mib=0, corrupt_index=None):
    active = {}

    async def database(app):
        init_shares(state)
        db = connect(state / "shares.sqlite3")
        try:
            app[database_key] = db
            yield
        finally:
            db.close()

    database_key = web.AppKey("share database", sqlite3.Connection)

    async def stats(request):
        return web.json_response({"active": list(active.values())})

    async def chunk(request):
        ident = request.match_info["file_id"]
        index = int(request.match_info["index"])
        request_id = str(UUID(request.headers.get("X-Request-ID", "")))
        check = request.headers.get("X-Chunk-SHA256")
        row = request.app[database_key].execute(
            "SELECT path, manifest FROM shares WHERE id=?", (ident,)
        ).fetchone()
        if row is None:
            raise web.HTTPNotFound(text="File is not shared")
        path, manifest_json = row
        manifest = Manifest.parse(json.loads(manifest_json))
        try:
            length = manifest.length(index)
        except IndexError as error:
            raise web.HTTPNotFound(text="Unknown chunk") from error
        if check != manifest.chunks[index]:
            raise web.HTTPPreconditionFailed(text="Chunk hash differs from manifest")
        try:
            source = Path(path).open("rb")
        except FileNotFoundError as error:
            raise web.HTTPNotFound(text="Shared file is missing") from error
        response = web.StreamResponse(headers={
            "Content-Type": "application/octet-stream", "Content-Length": str(length),
            "X-Request-ID": request_id, "X-Chunk-SHA256": check,
        })
        event = {"file_id": ident, "index": index, "request_id": request_id,
                 "bytes_written": 0, "length": length}
        token = object()
        active[token] = event
        try:
            with source:
                source.seek(index * manifest.chunk_size)
                await response.prepare(request)
                while event["bytes_written"] < length:
                    offset = event["bytes_written"]
                    block = source.read(min(BLOCK, length - offset))
                    if not block:
                        response.force_close()
                        break
                    if corrupt_index == index and offset == 0:
                        block = bytes([block[0] ^ 1]) + block[1:]
                    if rate_mib:
                        await asyncio.sleep(len(block) / (rate_mib * 1024 * 1024))
                    await response.write(block)
                    event["bytes_written"] += len(block)
                await response.write_eof()
        except (ConnectionError, TimeoutError):
            LOG.debug("Client disconnected for request %s", request_id)
        finally:
            del active[token]
        return response

    app = web.Application(middlewares=[bad_input])
    app.cleanup_ctx.append(database)
    app.add_routes([
        web.get("/health", health), web.get("/stats", stats),
        web.get("/chunks/{file_id}/{index}", chunk, allow_head=False),
    ])
    return app


async def register_peer(directory, peer, url):
    async with ClientSession(timeout=ClientTimeout(total=5), raise_for_status=True) as session:
        async with session.put(directory + "/peers", json={"peer": peer, "url": url}) as reply:
            await reply.json()
