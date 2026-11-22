"""Command-line entry point for directory, peer, and client operations."""

import argparse
import asyncio
import json
import logging
import math
import os
import shlex
import signal
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from .download import TransferError, download, status
from .model import HEX, MIB, build_manifest, disk_operation
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


def parser(session=None):
    root = argparse.ArgumentParser(
        prog=f"{session.id}>" if session else "python -m napster", description=__doc__
    )
    root.add_argument("--verbose", action="store_true", help="Log chunk commits and reuse")
    commands = root.add_subparsers(dest="command", required=True)
    if session is None:
        directory = commands.add_parser("directory", help="Run the persistent metadata service")
        directory.add_argument("--db", type=Path, default=Path("run/directory.sqlite3"))
        peer = commands.add_parser("peer", help="Serve local files and register the endpoint")
        peer.add_argument("--id", required=True)
        peer.add_argument("--state", type=Path, required=True)
        peer.add_argument("--advertise", help="HTTP origin reachable from downloading peers")
        peer.add_argument("--rate-mib", type=nonnegative, default=0,
                          help="MiB/s per request (zero disables throttling)")
        peer.add_argument("--corrupt-index", type=int, help="Flip one byte of this chunk for fault testing")
        peer.add_argument("--interactive", action="store_true", help="Accept commands at a peer prompt")
        for service, port in ((directory, 8080), (peer, 8101)):
            service.add_argument("--bind", default="127.0.0.1")
            service.add_argument("--port", type=int, default=port, help="Use zero for an available port")
    else:
        commands.add_parser("help", help="List commands (use COMMAND --help for details)")
        commands.add_parser("exit", help="Close the terminal and stop this peer")
        commands.add_parser("health", help="Check this peer and the directory")
    publish = commands.add_parser("publish", help="Publish a local file from a running peer")
    publish.add_argument("path", type=Path)
    publish.add_argument("--id", required=session is None,
                         default=session.id if session else None, help="ID of the running peer")
    publish.add_argument("--state", type=Path, required=session is None,
                         default=session.state if session else None,
                         help="State directory of the running peer")
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
    clients = [publish, search, get]
    if session is None:
        clients.append(peer)
    for command in clients:
        command.add_argument("--directory", default=session.directory if session else DEFAULT_DIRECTORY)
    return root


async def check_health(directory, peer_url):
    async def check(session, url):
        try:
            async with session.get(url + "/health", allow_redirects=False) as reply:
                reply.raise_for_status()
                body = await reply.json()
                if reply.status != 200 or not isinstance(body, dict) or body.get("status") != "ok":
                    raise ValueError("Unexpected health response")
                return {"url": url, "status": "ok"}
        except (ClientError, TimeoutError, ValueError, OSError) as error:
            return {"url": url, "status": "unreachable", "error": str(error)}

    async with ClientSession(timeout=ClientTimeout(total=5)) as session:
        directory_health, peer_health = await asyncio.gather(
            check(session, directory.rstrip("/")), check(session, peer_url)
        )
    return {"directory": directory_health, "peer": peer_health}


async def console(args, peer_url):
    """Read terminal input without blocking the peer's HTTP event loop."""
    command_parser = parser(args)
    reader = asyncio.StreamReader()
    loop = asyncio.get_running_loop()
    # Duplicate stdin so closing the console transport does not close the caller's fd.
    with os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0) as stdin:
        transport, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), stdin
        )
        try:
            print("Type help for commands. Use quotes around paths containing spaces.", flush=True)
            while True:
                print(f"{args.id}> ", end="", flush=True)
                line = await reader.readline()
                if not line:
                    print(flush=True)
                    return
                try:
                    words = shlex.split(line.decode(sys.stdin.encoding or "utf-8"))
                    if not words:
                        continue
                    command = command_parser.parse_args(words)
                    if command.verbose:
                        logging.getLogger().setLevel(logging.INFO)
                    if command.command == "exit":
                        return
                    if command.command == "help":
                        command_parser.print_help()
                        continue
                    if command.command == "health":
                        result = await check_health(args.directory, peer_url)
                    else:
                        result = await run(command)
                    print(json.dumps(result, indent=2), flush=True)
                except SystemExit:
                    # argparse help and syntax errors must leave the peer running.
                    continue
                except (ClientError, TimeoutError, TransferError, ValueError,
                        OSError, sqlite3.Error) as error:
                    print(f"Error: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        finally:
            transport.close()


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
    tasks = []
    try:
        site = web.TCPSite(runner, args.bind, args.port)
        await site.start()
        port = runner.addresses[0][1]
        host = f"[{args.bind}]" if ":" in args.bind else args.bind
        url = f"http://{host}:{port}"
        if not directory_service:
            await register_peer(args.directory.rstrip("/"), args.id, args.advertise or url)
        print(json.dumps({"ready": url}), flush=True)
        if not directory_service and args.interactive:
            # Wildcard bind addresses are unsuitable as destinations for local health checks.
            local_host = "[::1]" if ":" in args.bind else "127.0.0.1"
            local_url = f"http://{local_host}:{port}" if args.bind in ("0.0.0.0", "::") else url
            terminal = asyncio.create_task(console(args, local_url))
            tasks = [terminal, asyncio.create_task(stop.wait())]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if terminal.done():
                await terminal
        else:
            await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()


async def publish_file(args, directory):
    path = args.path.resolve(strict=True)
    name = args.name or path.name
    if len(name) > 255:
        raise ValueError("Publication name exceeds 255 characters")
    manifest = await disk_operation(build_manifest, path, args.chunk_size)
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
