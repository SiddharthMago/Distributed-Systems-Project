"""Command-line entry point for directory, peer, and client operations."""

import argparse
import asyncio
import json
import logging
import math
import signal
from contextlib import closing
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from .download import TransferError, download, status
from .model import HEX, MIB, build_manifest
from .service import connect, directory_app, init_shares, peer_app, register_peer

DEFAULT_DIRECTORY = "http://127.0.0.1:8080"


def positive(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("Expected an integer greater than zero")
    return value


def nonnegative(text):
    value = float(text)
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("Expected a finite number greater than or equal to zero")
    return value


def file_id(text):
    if not HEX.fullmatch(text):
        raise argparse.ArgumentTypeError("Expected a 64-digit lowercase file ID")
    return text


def parser():
    root = argparse.ArgumentParser(prog="python -m napster", description=__doc__)
    root.add_argument("--verbose", action="store_true", help="Log chunk commits and reuse")
    commands = root.add_subparsers(dest="command", required=True)
    directory = commands.add_parser("directory", help="Run the persistent metadata service")
    directory.add_argument("--db", type=Path, default=Path("run/directory.sqlite3"))
    peer = commands.add_parser("peer", help="Serve local files and register the endpoint")
    peer.add_argument("--id", required=True)
    peer.add_argument("--state", type=Path, required=True)
    peer.add_argument("--advertise", help="HTTP origin reachable from downloading peers")
    peer.add_argument("--rate-mib", type=nonnegative, default=0,
                      help="MiB/s per request (zero disables throttling)")
    peer.add_argument("--corrupt-index", type=int, help="Flip one byte of this chunk for fault testing")
    for service, port in ((directory, 8080), (peer, 8101)):
        service.add_argument("--bind", default="127.0.0.1")
        service.add_argument("--port", type=int, default=port, help="Use zero for an available port")
    publish = commands.add_parser("publish", help="Publish a local file from a running peer")
    publish.add_argument("path", type=Path)
    publish.add_argument("--id", required=True, help="ID of the running peer")
    publish.add_argument("--state", type=Path, required=True, help="State directory of the running peer")
    publish.add_argument("--name", help="Searchable name, defaults to the basename")
    publish.add_argument("--chunk-size", type=positive, default=MIB, help="Chunk size in bytes")
    search = commands.add_parser("search", help="Find publications by name or file ID")
    search.add_argument("query", nargs="?", default="")
    get = commands.add_parser("download", help="Download or resume into an explicit output path")
    get.add_argument("file_id", type=file_id)
    get.add_argument("output", type=Path)
    get.add_argument("--workers", type=positive, default=4)
    get.add_argument("--timeout", type=nonnegative, default=10, help="Seconds per HTTP operation")
    get.add_argument("--rounds", type=positive, default=3, help="Source passes per chunk")
    progress = commands.add_parser("status", help="Report progress and payload counters")
    progress.add_argument("output", type=Path)
    progress.add_argument("--details", action="store_true")
    for command in (peer, publish, search, get):
        command.add_argument("--directory", default=DEFAULT_DIRECTORY)
    return root


async def serve(args):
    directory_service = args.command == "directory"
    app = directory_app(args.db) if directory_service else peer_app(
        args.state, rate_mib=args.rate_mib, corrupt_index=args.corrupt_index
    )
    runner = web.AppRunner(app, handler_cancellation=True, access_log=None)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await runner.setup()
    try:
        site = web.TCPSite(runner, args.bind, args.port)
        await site.start()
        port = runner.addresses[0][1]
        host = f"[{args.bind}]" if ":" in args.bind else args.bind
        url = f"http://{host}:{port}"
        if not directory_service:
            await register_peer(args.directory.rstrip("/"), args.id, args.advertise or url)
        print(json.dumps({"ready": url}), flush=True)
        await stop.wait()
    finally:
        await runner.cleanup()


async def publish_file(args, directory):
    path = args.path.resolve(strict=True)
    name = args.name or path.name
    if len(name) > 255:
        raise ValueError("Publication name exceeds 255 characters")
    manifest = build_manifest(path, args.chunk_size)
    init_shares(args.state)
    with closing(connect(args.state / "shares.sqlite3")) as db:
        db.execute(
            "INSERT INTO shares VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET path=excluded.path",
            (manifest.id, str(path), json.dumps(manifest.to_dict())),
        )
    async with ClientSession(timeout=ClientTimeout(total=10), raise_for_status=True) as session:
        async with session.post(directory + "/publications", json={
            "peer": args.id, "name": name, "manifest": manifest.to_dict(),
        }) as reply:
            return await reply.json()


async def run(args):
    if args.command in ("directory", "peer"):
        await serve(args)
        return None
    if args.command == "status":
        return status(args.output, args.details)
    directory = args.directory.rstrip("/")
    if args.command == "publish":
        return await publish_file(args, directory)
    if args.command == "search":
        async with ClientSession(timeout=ClientTimeout(total=10), raise_for_status=True) as session:
            async with session.get(directory + "/search", params={"q": args.query}) as reply:
                return await reply.json()
    if args.timeout == 0:
        raise ValueError("Timeout must be greater than zero")
    return await download(directory, args.file_id, args.output, args.workers, args.timeout, args.rounds)


def main():
    args = parser().parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        result = asyncio.run(run(args))
    except (ClientError, TransferError, ValueError, OSError) as error:
        logging.error("%s: %s", type(error).__name__, error)
        return 1
    except KeyboardInterrupt:
        return 130
    if result is not None:
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
