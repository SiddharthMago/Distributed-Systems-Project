"""Local multi-process harness shared by the experiment and integration tests."""

import json
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from urllib.request import urlopen


def stop(process, hard=False):
    if process.poll() is None:
        (process.kill if hard else process.terminate)()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def wait_for(check, timeout=30, interval=0.02):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = check()
        if result:
            return result
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout} seconds waiting for lab condition")


class Lab:
    def __init__(self, root):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.stack = ExitStack()
        self.peers = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self.stack.__exit__(*exc)

    @staticmethod
    def command(*args):
        return [sys.executable, "-m", "napster", *map(str, args)]

    def run(self, *args):
        run = subprocess.run(self.command(*args), capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(f"{args[0]} failed: {run.stderr.strip()}")
        return json.loads(run.stdout)

    def spawn(self, name, *args, ready=False):
        stdout_path = self.root / f"{name}.stdout.log"
        stderr_path = self.root / f"{name}.stderr.log"
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            process = subprocess.Popen(self.command(*args), stdout=stdout, stderr=stderr)
        self.stack.callback(stop, process)
        if not ready:
            return process

        def read_ready():
            if process.poll() is not None:
                raise RuntimeError(f"{name} failed to start: {stderr_path.read_text()}")
            raw = stdout_path.read_text()
            if raw.endswith("\n"):
                return json.loads(raw)["ready"]
            return None

        return process, wait_for(read_ready)

    def start_directory(self, port=0):
        self.directory_process, self.directory = self.spawn(
            "directory", "directory", "--db", self.root / "directory.sqlite3",
            "--port", port, ready=True,
        )

    def start_peer(self, name, rate=0, corrupt=None):
        extra = [] if corrupt is None else ["--corrupt-index", corrupt]
        process, url = self.spawn(name, "peer", "--id", name, "--state", self.root / name,
                                  "--port", 0, "--directory", self.directory,
                                  "--rate-mib", rate, *extra, ready=True)
        self.peers[name] = (process, url)

    def publish(self, name, path, chunk_size=1024 * 1024):
        return self.run("publish", path, "--id", name, "--state", self.root / name,
                        "--directory", self.directory, "--chunk-size", chunk_size)["file_id"]

    def downloader(self, name, ident, output, *extra):
        return self.spawn(name, "--verbose", "download", ident, output,
                          "--directory", self.directory, *extra)

    def result(self, name, process, timeout=120):
        process.wait(timeout=timeout)
        if process.returncode:
            raise RuntimeError((self.root / f"{name}.stderr.log").read_text())
        return json.loads((self.root / f"{name}.stdout.log").read_text())

    def active(self, name):
        with urlopen(self.peers[name][1] + "/stats", timeout=2) as reply:
            return json.load(reply)["active"]


def make_payload(path: Path, size: int, seed=17):
    from random import Random

    rng = Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        remaining = size
        while remaining:
            length = min(1024 * 1024, remaining)
            stream.write(rng.randbytes(length))
            remaining -= length
