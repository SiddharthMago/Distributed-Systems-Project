import asyncio
import json
import logging
from collections import Counter
from contextlib import closing
from dataclasses import replace

import pytest

from napster import download as client
from napster.lab import Lab, make_payload, stop, wait_for
from napster.model import MIB, Manifest, build_manifest, sha256_file
from napster.service import connect


def progress(output, minimum):
    try:
        snapshot = client.status(output, details=True)
    except client.TransferError:
        return None
    return snapshot if snapshot["verified_chunks"] >= minimum else None


def test_persistent_directory_and_tail_chunk(tmp_path):
    source = tmp_path / "Source.TXT"
    output = tmp_path / "download.bin"
    make_payload(source, 150_003)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice")
        lab.start_peer("bob")
        ident = lab.publish("alice", source, 65536)
        assert lab.publish("bob", source, 65536) == ident
        assert lab.publish("alice", source, 65536) == ident
        stop(lab.directory_process, hard=True)
        lab.start_directory()
        matches = lab.run("search", "source.txt", "--directory", lab.directory)
        assert matches == [{"file_id": ident, "name": source.name, "sources": 2}]
        stop(lab.peers["bob"][0])
        lab.start_peer("bob")
        result = lab.result("download", lab.downloader("download", ident, output))
        assert result["verified_chunks"] == result["commits"] == 3
        assert {row["peer"] for row in result["peers"] if row["verified_chunks"]} == {"alice", "bob"}
        assert sha256_file(output) == sha256_file(source)
        assert result["retransmitted_bytes"] == 0


def test_killed_downloader_recovers_commits_and_partial_request_id(tmp_path):
    source = tmp_path / "test.bin"
    output = tmp_path / "download.bin"
    make_payload(source, 12 * MIB + 23)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("source", rate=4)
        ident = lab.publish("source", source)
        first = lab.downloader("first", ident, output, "--workers", 2)
        wait_for(lambda: progress(output, 2))

        def inflight():
            return [chunk for chunk in lab.active("source")
                    if 0 < chunk["bytes_written"] < chunk["length"]]

        partial = wait_for(inflight)
        stop(first, hard=True)
        before = client.status(output, details=True)
        committed = {chunk["idx"] for chunk in before["chunks"] if chunk["complete"]}
        missing = {chunk["idx"]: chunk["request_id"] for chunk in before["chunks"]
                   if not chunk["complete"]}
        assert any(missing.get(chunk["index"]) == chunk["request_id"] for chunk in partial)
        result = lab.result("resume", lab.downloader("resume", ident, output, "--workers", 4))
        after = client.status(output, details=True)
        assert result["resumed_chunks"] >= len(committed)
        assert after["verified_chunks"] == after["commits"] == 13
        assert all(chunk["request_id"] == before["chunks"][chunk["idx"]]["request_id"]
                   for chunk in after["chunks"])
        attempts = Counter(row["idx"] for row in after["attempts"])
        assert all(attempts[index] == 1 for index in committed)
        log = (lab.root / "resume.stderr.log").read_text()
        assert all(f"reuse chunk={index} " in log for index in committed)
        assert sha256_file(output) == sha256_file(source)


def test_corrupt_source_retries_with_same_request_id(tmp_path):
    source = tmp_path / "test.bin"
    output = tmp_path / "download.bin"
    size = 256 * 1024
    make_payload(source, size + 41)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice", corrupt=0)
        lab.start_peer("bob")
        ident = lab.publish("alice", source, size)
        assert lab.publish("bob", source, size) == ident
        result = lab.result("download", lab.downloader("download", ident, output))
        assert result["retransmitted_bytes"] == size
        snapshot = client.status(output, details=True)
        retries = [attempt for attempt in snapshot["attempts"] if attempt["idx"] == 0]
        assert [(row["peer"], row["outcome"]) for row in retries] == [
            ("alice", "TransferError"), ("bob", "verified"),
        ]
        request_id = snapshot["chunks"][0]["request_id"]
        log = (lab.root / "download.stderr.log").read_text()
        assert f"request_id={request_id} peer=alice" in log
        assert f"request_id={request_id} peer=bob" in log
        assert snapshot["commits"] == 2
        assert sha256_file(output) == sha256_file(source)


def test_restart_adopts_orphan_and_invalidates_damaged_chunk(tmp_path, caplog):
    source = tmp_path / "fixture"
    make_payload(source, 300)
    manifest = build_manifest(source, 100)
    state = tmp_path / "checkpoint"
    state.mkdir()
    with closing(connect(state / "checkpoint.sqlite3", client.SCHEMA)) as db:
        checkpoint = client.Checkpoint(state, db, manifest.id, manifest)
        original = source.read_bytes()
        checkpoint.path(0).write_bytes(original[:100])
        checkpoint.path(1).write_bytes(b"Z" * 100)
        checkpoint.commit(1)
        checkpoint.path(2).with_suffix(".tmp").write_bytes(original[200:])
        caplog.set_level(logging.INFO)
        assert [index for index, _ in checkpoint.reconcile()] == [1, 2]
        assert "reuse chunk=0" in caplog.text
        assert "invalidate saved chunk=1" in caplog.text
        assert not checkpoint.path(2).with_suffix(".tmp").exists()
        assert db.execute("SELECT complete FROM chunks ORDER BY idx").fetchall() == [(1,), (0,), (0,)]


def test_empty_file_and_failed_whole_hash_never_replace_output(tmp_path):
    source = tmp_path / "empty"
    output = tmp_path / "output"
    source.touch()
    manifest = build_manifest(source)
    checkpoint_root = tmp_path / "checkpoint"
    checkpoint_root.mkdir()
    invalid = replace(manifest, sha256="f" * 64)
    with closing(connect(checkpoint_root / "checkpoint.sqlite3", client.SCHEMA)) as db:
        checkpoint = client.Checkpoint(checkpoint_root, db, invalid.id, invalid)
        output.write_text("existing output")
        with pytest.raises(client.TransferError, match="Reconstructed file"):
            client.assemble(checkpoint, output)
        assert output.read_text() == "existing output"
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice")
        ident = lab.publish("alice", source)
        result = lab.result("empty", lab.downloader("empty", ident, output))
        assert result["hash_equal"]
        assert result["total_chunks"] == result["payload_bytes"] == 0
        assert output.read_bytes() == b""


def test_manifest_and_output_lock(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"xyz")
    manifest = build_manifest(path, 2)
    decoded = json.loads(json.dumps(manifest.to_dict()))
    assert Manifest.parse(decoded) == manifest
    decoded["chunks"].pop()
    with pytest.raises(ValueError, match="chunk count"):
        Manifest.parse(decoded)
    with client.exclusive_job(tmp_path):
        with pytest.raises(client.TransferError, match="already holds"):
            with client.exclusive_job(tmp_path):
                pytest.fail("Second process obtained the job lock")


def test_worker_count_bounds_concurrent_chunk_attempts(monkeypatch, tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"x" * 16)
    manifest = build_manifest(path, 1)
    current = high_watermark = 0

    async def fake_fetch(session, checkpoint, index, request_id, source):
        nonlocal current, high_watermark
        current += 1
        high_watermark = max(current, high_watermark)
        await asyncio.sleep(0)
        current -= 1
        return True, False

    monkeypatch.setattr(client, "fetch", fake_fetch)
    with closing(connect(tmp_path / "checkpoint.sqlite3", client.SCHEMA)) as db:
        checkpoint = client.Checkpoint(tmp_path, db, manifest.id, manifest)
        sources = [{"peer": code, "url": code} for code in ("alice", "bob")]
        asyncio.run(client.transfer(None, "", checkpoint, checkpoint.reconcile(), sources, 4, 1))
    assert high_watermark == 4


def test_exhausted_sources_leave_resumable_partial_job(tmp_path):
    source = tmp_path / "test.bin"
    output = tmp_path / "download.bin"
    make_payload(source, 129)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice", corrupt=0)
        ident = lab.publish("alice", source, 128)
        first = lab.downloader("first", ident, output, "--workers", 2, "--rounds", 1)
        with pytest.raises(RuntimeError, match="Sources exhausted"):
            lab.result("first", first)
        assert not output.exists()
        before = client.status(output, details=True)
        assert before["verified_chunks"] == 1
        assert before["chunks"][0]["complete"] == 0
        lab.start_peer("bob")
        lab.publish("bob", source, 128)
        after = lab.result("resume", lab.downloader("resume", ident, output))
        assert after["resumed_chunks"] == 1
        assert after["commits"] == 2
        assert after["hash_equal"]
        assert sha256_file(output) == sha256_file(source)
