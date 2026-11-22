"""Exercise interactive peer commands through real subprocess stdin and HTTP."""

import asyncio
import json
import os
import pty
import shlex
import signal
import subprocess
import threading
from pathlib import Path
from urllib.request import urlopen

import pytest

from napster import __main__ as cli
from napster.download import TransferError, status
from napster.lab import Lab, make_payload, stop, wait_for
from napster.model import MIB, build_manifest, disk_operation, sha256_file


def start_console(lab, name="console"):
    stdout_path = lab.root / f"{name}.stdout.log"
    stderr_path = lab.root / f"{name}.stderr.log"
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = subprocess.Popen(
            lab.command("--verbose", "peer", "--interactive", "--id", name,
                        "--state", lab.root / name, "--port", 0, "--directory", lab.directory),
            stdin=subprocess.PIPE, stdout=stdout, stderr=stderr, text=True,
        )
    lab.stack.callback(process.stdin.close)
    lab.stack.callback(stop, process)

    def ready():
        if process.poll() is not None:
            raise RuntimeError(stderr_path.read_text())
        text = stdout_path.read_text()
        if f"{name}> " in text:
            return json.loads(text.splitlines()[0])["ready"]
        return None

    url = wait_for(ready)
    return process, url, stdout_path, stderr_path


def send(process, command):
    process.stdin.write(command + "\n")
    process.stdin.flush()


def saved_progress(output, minimum):
    try:
        snapshot = status(output)
    except TransferError:
        return None
    return snapshot if snapshot["verified_chunks"] >= minimum else None


def test_console_commands_defaults_quotes_errors_and_self_transfer(tmp_path):
    source = tmp_path / "a quoted filename.bin"
    output = tmp_path / "download with spaces.bin"
    make_payload(source, MIB + 19)
    ident = build_manifest(source, 262144).id
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        process, _, stdout, stderr = start_console(lab)
        commands = [
            "", "help", "publish --help", "unknown", 'publish "unterminated',
            "download invalid-id output", "status missing-file",
            "publish missing-file", "health",
            f"publish {shlex.quote(str(source))} --chunk-size 262144 --name 'A friendly name'",
            "search 'friendly name'",
            f"download {ident} {shlex.quote(str(output))} --workers 2",
            f"status {shlex.quote(str(output))} --details",
            f"download {ident} {shlex.quote(str(output))}",
            "health", "exit",
        ]
        for command in commands:
            send(process, command)
        process.wait(timeout=30)
        assert process.returncode == 0, stderr.read_text()
        text = stdout.read_text()
        assert '"name": "A friendly name"' in text
        assert '"hash_equal": true' in text
        assert '"resumed_chunks": 5' in text
        assert '"status": "ok"' in text
        assert "No closing quotation" in stderr.read_text()
        assert "invalid choice" in stderr.read_text()
        assert sha256_file(output) == sha256_file(source)
        snapshot = status(output, details=True)
        assert snapshot["total_chunks"] == snapshot["commits"] == 5
        assert snapshot["payload_bytes"] == source.stat().st_size


def test_peer_serves_while_prompt_idle_and_stops_on_signal(tmp_path):
    source = tmp_path / "source"
    output = tmp_path / "received"
    make_payload(source, MIB)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        process, url, stdout, stderr = start_console(lab)
        send(process, f"publish {shlex.quote(str(source))}")
        ident = build_manifest(source).id
        wait_for(lambda: ident in stdout.read_text())
        # stdin remains open with no further input while an external client downloads.
        result = lab.result("outside", lab.downloader("outside", ident, output))
        assert result["hash_equal"]
        with urlopen(url + "/health", timeout=2) as reply:
            assert json.load(reply)["status"] == "ok"
        stop(lab.directory_process)
        send(process, "health")
        wait_for(lambda: '"status": "unreachable"' in stdout.read_text())
        assert '"peer": {' in stdout.read_text()
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)
        assert process.returncode == 0, stderr.read_text()


def test_interactive_corruption_retry_and_eof(tmp_path):
    source = tmp_path / "source"
    output = tmp_path / "received"
    make_payload(source, MIB + 11)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice", corrupt=0)
        lab.start_peer("bob")
        ident = lab.publish("alice", source)
        lab.publish("bob", source)
        process, _, stdout, stderr = start_console(lab)
        send(process, f"download {ident} {shlex.quote(str(output))}")
        # EOF exits after the submitted command finishes.
        process.stdin.close()
        process.wait(timeout=30)
        assert process.returncode == 0, stderr.read_text()
        assert '"hash_equal": true' in stdout.read_text()
        snapshot = status(output, details=True)
        assert snapshot["retransmitted_bytes"] == MIB
        assert snapshot["commits"] == snapshot["total_chunks"] == 2
        assert [(row["peer"], row["outcome"]) for row in snapshot["attempts"]
                if row["idx"] == 0] == [("alice", "TransferError"), ("bob", "verified")]


def test_interactive_source_loss_and_cancelled_download_resume(tmp_path):
    source = tmp_path / "source"
    output = tmp_path / "received"
    make_payload(source, 12 * MIB + 17)
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice", rate=0.5)
        lab.start_peer("bob", rate=0.5)
        ident = lab.publish("alice", source)
        lab.publish("bob", source)
        process, url, _, stderr = start_console(lab)
        send(process, f"download {ident} {shlex.quote(str(output))}")
        wait_for(lambda: saved_progress(output, 4))
        stop(lab.peers["alice"][0], hard=True)
        # The peer can still handle requests during its foreground download.
        with urlopen(url + "/health", timeout=2) as reply:
            assert json.load(reply)["status"] == "ok"
        process.send_signal(signal.SIGINT)
        process.wait(timeout=5)
        assert process.returncode == 0, stderr.read_text()
        before = status(output, details=True)
        assert 0 < before["verified_chunks"] < before["total_chunks"]
        resumed, _, stdout, resumed_errors = start_console(lab, "resumed")
        send(resumed, f"download {ident} {shlex.quote(str(output))}")
        send(resumed, "exit")
        resumed.wait(timeout=30)
        assert resumed.returncode == 0, resumed_errors.read_text()
        assert '"hash_equal": true' in stdout.read_text()
        after = status(output, details=True)
        assert after["commits"] == after["total_chunks"] == 13
        assert [item["request_id"] for item in before["chunks"]] == [
            item["request_id"] for item in after["chunks"]
        ]
        assert "reuse chunk=" in resumed_errors.read_text()
        assert sha256_file(output) == sha256_file(source)


def test_publication_hashing_yields_to_other_async_work(monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"content")
    started = threading.Event()
    release = threading.Event()
    original = cli.build_manifest

    def slow_manifest(*args):
        started.set()
        assert release.wait(timeout=5)
        return original(*args)

    monkeypatch.setattr(cli, "build_manifest", slow_manifest)

    async def exercise(directory):
        args = cli.parser().parse_args([
            "publish", str(source), "--id", "alice", "--state", str(tmp_path / "shares"),
            "--directory", directory,
        ])
        task = asyncio.create_task(cli.publish_file(args, directory))
        try:
            while not started.is_set():
                await asyncio.sleep(0.01)
            # The event loop must progress while the hashing thread is waiting.
            assert not task.done()
            release.set()
            return await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        lab.start_peer("alice")
        assert asyncio.run(exercise(lab.directory))["file_id"] == original(source).id


def test_cancelled_disk_work_finishes_before_resources_are_released():
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def work():
        started.set()
        release.wait(timeout=5)
        finished.set()

    async def exercise():
        task = asyncio.create_task(disk_operation(work))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(exercise())


def test_terminal_device_accepts_commands_and_ctrl_d(tmp_path):
    with Lab(tmp_path / "cluster") as lab:
        lab.start_directory()
        master, slave = pty.openpty()
        lab.stack.callback(os.close, master)
        stdout_path = tmp_path / "terminal.stdout"
        stderr_path = tmp_path / "terminal.stderr"
        try:
            with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
                process = subprocess.Popen(
                    lab.command("peer", "--interactive", "--id", "tty",
                                "--state", tmp_path / "tty", "--port", 0,
                                "--directory", lab.directory),
                    stdin=slave, stdout=stdout, stderr=stderr,
                )
        finally:
            os.close(slave)
        lab.stack.callback(stop, process)
        wait_for(lambda: "tty> " in stdout_path.read_text())
        os.write(master, b"health\n")
        wait_for(lambda: '"status": "ok"' in stdout_path.read_text())
        os.write(master, b"\x04")
        process.wait(timeout=5)
        assert process.returncode == 0, stderr_path.read_text()
