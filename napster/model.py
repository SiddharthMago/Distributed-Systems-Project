"""Content manifests and durable filesystem primitives."""

import asyncio
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path

MIB = 1024 * 1024
BLOCK = 64 * 1024
MAX_CHUNK = 16 * MIB
HEX = re.compile(r"[0-9a-f]{64}")


async def disk_operation(operation, *args):
    """Keep disk work off the HTTP loop and finish it before releasing job resources."""
    task = asyncio.create_task(asyncio.to_thread(operation, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        finally:
            raise


@dataclass(frozen=True)
class Manifest:
    size: int
    chunk_size: int
    sha256: str
    chunks: tuple[str, ...]

    def to_dict(self):
        return asdict(self)

    @property
    def id(self):
        encoded = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()

    def length(self, index):
        if not 0 <= index < len(self.chunks):
            raise IndexError(index)
        return min(self.chunk_size, self.size - index * self.chunk_size)

    @classmethod
    def parse(cls, data):
        size, chunk_size = data["size"], data["chunk_size"]
        if type(size) is not int or size < 0:
            raise ValueError("Invalid file size")
        if type(chunk_size) is not int or not 1 <= chunk_size <= MAX_CHUNK:
            raise ValueError("Invalid chunk size")
        count = (size + chunk_size - 1) // chunk_size
        if not isinstance(data["chunks"], list) or len(data["chunks"]) != count:
            raise ValueError("Invalid chunk count")
        hashes = (data["sha256"], *data["chunks"])
        if any(not isinstance(value, str) or not HEX.fullmatch(value) for value in hashes):
            raise ValueError("Invalid SHA-256 value")
        return cls(size, chunk_size, hashes[0], tuple(hashes[1:]))


def build_manifest(path, chunk_size=MIB):
    if not 1 <= chunk_size <= MAX_CHUNK:
        raise ValueError(f"Chunk size must be 1 to {MAX_CHUNK} bytes")
    whole = hashlib.sha256()
    chunks = []
    size = 0
    with Path(path).open("rb") as source:
        while block := source.read(chunk_size):
            size += len(block)
            whole.update(block)
            chunks.append(hashlib.sha256(block).hexdigest())
    return Manifest(size, chunk_size, whole.hexdigest(), tuple(chunks))


def sha256_file(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_replace(temp, target):
    os.replace(temp, target)
    sync_directory(target.parent)
    if target.parent != temp.parent:
        sync_directory(temp.parent)
