# Napster with Reliable File Transfers

Distributed Systems course project · Ananth Rajesh, Mithun Rameshbabu, and Siddharth Mago

A SQLite-backed directory indexes file manifests and peer addresses. Six peers can publish files and stream chunks directly to downloaders. The downloader uses four concurrent workers by default, verifies each chunk with SHA-256, and uses its on-disk checkpoint to resume after a restart.

See [architecture and protocol](PROTOCOL.md) for message formats, retry behavior, and crash recovery. The [experiment report](results/REPORT.md) includes all six measured runs, with the [CSV](results/measurements.csv) and [fault evidence](results/evidence.json) alongside it.

## Setup

Use Python 3.12 on macOS or Linux. From the project root, create a virtual environment and install the project and analysis dependencies.

```sh
make setup
make test
```

These targets use the virtual environment's Python directly, so activation is
unnecessary. GNU Make is available through the command-line developer tools on
macOS and the `make` package on Linux or WSL. To use a different Python executable,
run `make setup PYTHON=python3` with Python 3.12 installed.

The equivalent commands without Make are:

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Use `requirements.txt` for the directory, peer, and client CLI only. The tests and local experiment bind temporary loopback ports. A four-core machine with 8 GB of RAM and 10 GB of free disk space is a planning estimate. The default experiment needs roughly 500 MiB for payload files at peak, plus logs, SQLite files, and Python dependencies. A 1 GiB trial needs roughly 5 GiB for payload files at peak. An RCE account is unnecessary for the local procedure.

## Start services with Make

Copy the configuration example once on each PC and edit the local values.

```sh
cp .env.example .env
```

The local `.env` is ignored by Git. It uses Make assignment syntax with unquoted
values, such as `ID=peer1` and `IP=192.168.1.10`. Command-line assignments override
the file, and omitted settings use the defaults at the top of the Makefile.
Leave derived settings such as `STATE`, `ADVERTISE`, and `LOG` commented out to
have them follow the chosen ID, IP, and port.

For a local demonstration, start the directory in one terminal and each peer in
its own terminal.

```sh
make directory
make peer
make peer ID=peer2 PORT=8102
```

For three PCs, set `IP` to each PC's reachable LAN or VPN address and
`DIRECTORY_IP` to PC A's address on all PCs. This table gives an example layout.

| PC | Example IP | Commands in separate terminals |
| --- | --- | --- |
| A | 192.168.1.10 | `make directory`, `make peer ID=peer1 PORT=8101`, `make peer ID=peer4 PORT=8104` |
| B | 192.168.1.11 | `make peer ID=peer2 PORT=8102`, `make peer ID=peer5 PORT=8105` |
| C | 192.168.1.12 | `make peer ID=peer3 PORT=8103`, `make peer ID=peer6 PORT=8106` |

Start the directory first. You can supply the same settings directly without
creating a configuration file.

```sh
make peer ID=peer2 IP=192.168.1.11 PORT=8102 DIRECTORY_IP=192.168.1.10
```

`IP` is the address advertised to other peers. `BIND=0.0.0.0` controls listening
on all local IPv4 interfaces. Use a reachable IP for advertising. Each peer on a
given PC needs a distinct port and state directory.

| Setting | Default | Purpose |
| --- | --- | --- |
| `ID` | `peer1` | Peer identity |
| `IP` | `127.0.0.1` | Advertised peer address |
| `BIND` | `0.0.0.0` | Local listening interface |
| `PORT` | `8101` | Peer listening port |
| `DIRECTORY_IP` | `127.0.0.1` | Directory address used by peers |
| `DIRECTORY_PORT` | `8080` | Directory listening and connection port |
| `STATE` | `run/manual/$(ID)` | Persistent peer state |
| `DB` | `run/manual/directory.sqlite3` | Persistent directory database |
| `RATE` | `0.5` | MiB/s per outgoing request, zero disables throttling |
| `LOG` | `run/manual/$(ID).log` | Append diagnostic logs, use `LOG=` to show them in the terminal |
| `VERBOSE` | `1` | Log commits and retries, use `0` to disable |
| `INTERACTIVE` | `1` | Enable the peer prompt, use `0` for server-only mode |
| `CORRUPT_INDEX` | empty | Deliberately corrupt this chunk for testing |
| `DIRECTORY_URL` | Derived from directory IP and port | Override the complete directory origin |
| `ADVERTISE` | Derived from peer IP and port | Override the complete advertised origin |
| `VENV` | `.venv` | Python environment directory |
| `PYTHON` | `python3.12` | Interpreter used by `make setup` |

For example, `make peer CORRUPT_INDEX=0` enables the corruption test and
`make peer RATE=0 LOG=` starts an unthrottled peer with visible diagnostics.
The prompt accepts `publish`, `search`, `download`, `status`, and `health` as
described below. Ctrl-C stops a running service. `make help` lists the targets.

## Reproduce the failure experiment

```sh
make experiment
```

The command launches one directory plus six peer processes for each run, publishes a deterministic 100 MiB file on peers 1 and 2, and downloads into peer 6's state directory. It runs three baselines and three peer-loss trials. In each peer-loss trial it stops peer 1 with SIGKILL once at least half the file is verified and peer 1 has an active request with a partially written body. It checks that the unfinished chunk was retried on peer 2, previously committed chunks were requested only once, each chunk was committed once, and the final hash equals the original fixture hash. The work directory retains process logs and SQLite evidence. The driver removes completed trial payload copies after verifying them to limit disk use.

Outputs go to `run/results/` with logs under `run/experiment/`. Use a fresh work directory each time. For a shorter demonstration, use the following settings.

```sh
python -m napster.experiment --size-mib 16 --trials 1 \
  --work-dir run/quick-demo --results run/quick-results
```

## Interactive peer terminals

Start the directory in its own terminal.

```sh
python -m napster directory --db run/manual/directory.sqlite3
```

Start each peer in a separate terminal with `--interactive`. Its identity, state directory,
and directory URL are remembered for commands entered at the prompt.

```sh
python -m napster --verbose peer --interactive --id peer1 \
  --state run/manual/peer1 --port 8101
```

For a second local peer, use ID `peer2`, state `run/manual/peer2`, and port `8102`.
For different PCs, provide reachable addresses once at startup.

```sh
python -m napster --verbose peer --interactive --id peer2 \
  --state run/manual/peer2 --bind 0.0.0.0 --port 8101 \
  --advertise http://THIS_PC_IP:8101 --directory http://DIRECTORY_PC_IP:8080
```

Replace the IP placeholders with reachable LAN or VPN addresses. Run these commands
from the project root on each PC after activating its virtual environment.

At the peer prompt, enter commands directly. Here `FILE_ID` means the actual ID
returned by publish or search.

```text
peer1> health
peer1> publish "path/to/sample.bin"
peer1> search sample
peer1> download FILE_ID "run/manual/received.bin" --workers 4
peer1> status "run/manual/received.bin" --details
peer1> help
peer1> publish --help
peer1> exit
```

The source path must exist on the PC running that peer. Paths are relative to the
directory from which the process was started. Quote paths and search queries
containing spaces. Type the command after the displayed prompt. Commands accept
their existing options, including `publish --chunk-size`, `download --timeout`,
and `download --rounds`. The `health` command checks both the local peer server
and the configured directory and reports unreachable endpoints individually.

The terminal runs one command at a time while the peer continues serving HTTP
requests. Verbose mode prints chunk commits, retries, and checkpoint reuse.
To inspect progress during a foreground download, run the existing `status`
command in another terminal. Syntax errors and failed commands return to the
prompt. `exit`, Ctrl-D at an empty prompt, or Ctrl-C stop the interactive peer.
After an interrupted download, restart the peer and repeat `download` with the
same file ID and output path to reuse its checkpoint.

Interactive terminals use the same macOS/Linux environment as the services.
Windows users can run them inside WSL. Start peers without `--interactive`
for background processes, scripts, and Docker Compose.

## Command-line services and clients

In one terminal, run the directory.

```sh
python -m napster directory --db run/manual/directory.sqlite3
```

In a second terminal, start six peers. In Bash or Zsh you can stop them at the end with `kill $(jobs -p)`.

```sh
for n in 1 2 3 4 5 6; do
  python -m napster peer --id "peer$n" --port "$((8100 + n))" \
    --state "run/manual/peer$n" &
done
```

After all six peers print their ready messages, publish a file from two peers and download it into peer 6's directory. Use the same `--state` and `--id` as each running peer. The publisher and downloader commands run on the respective peer hosts in a multi-host deployment.

```sh
printf 'Napster transfer example\n' > run/manual/example.txt
python -m napster publish run/manual/example.txt --id peer1 --state run/manual/peer1
python -m napster publish run/manual/example.txt --id peer2 --state run/manual/peer2
python -m napster search example.txt
FILE_ID=$(python -m napster search example.txt |
  python -c 'import json, sys; print(json.load(sys.stdin)[0]["file_id"])')
python -m napster --verbose download "$FILE_ID" run/manual/peer6/example.txt
python -m napster status run/manual/peer6/example.txt --details
cmp run/manual/example.txt run/manual/peer6/example.txt
```

Rerun the same download command after interrupting it. It validates the saved chunks and schedules missing or damaged chunks. `status` reports the checkpoint, while the successful `download` result reports the reconstructed file hash and elapsed seconds. The sidecar directory is next to the output and ends in `.napster/`. Keep it to preserve progress. Remove it after a successful download to reclaim the stored chunk space.

Use `--chunk-size` on publication to change the default from 1,048,576 bytes. Use `--workers`, `--timeout`, and `--rounds` on download to configure the parallel limit, timeout per HTTP operation, and retry passes. Use `--directory` on peer and client commands to override the directory origin. On multiple hosts, start each service with an appropriate `--bind` address and set each peer's `--advertise` to an origin reachable from the other peers.

## Docker Compose

Use Docker Compose v2 from the project root. The same application code runs in seven containers. Named volumes preserve the directory database, peer share metadata, and files saved under `/data`. Peer addresses use Compose service names.

```sh
docker compose up -d --build
docker compose logs directory peer1 peer2 peer6
printf 'Napster Compose example\n' > compose-example.txt
docker compose cp compose-example.txt peer1:/data/example.txt
docker compose cp compose-example.txt peer2:/data/example.txt
for peer in peer1 peer2; do
  docker compose exec -T "$peer" python -m napster publish /data/example.txt \
    --id "$peer" --state /data --directory http://directory:8080
done
FILE_ID=$(docker compose exec -T peer6 python -m napster search example.txt \
  --directory http://directory:8080 |
  python -c 'import json, sys; print(json.load(sys.stdin)[0]["file_id"])')
docker compose exec -T peer6 python -m napster --verbose download "$FILE_ID" \
  /data/received.txt --directory http://directory:8080
docker compose cp peer6:/data/received.txt received.txt
cmp compose-example.txt received.txt
docker compose down
```

The local test suite and six-peer experiment were executed on macOS. The Compose configuration is supplied for deployment and requires a host with Docker to exercise it.

## Assumptions

- This is a trusted lab network with one persistent directory. Start the directory before peers. Search results count registered publications and may include offline peers. Clients discover live sources by trying the advertised addresses. After a directory restart, its SQLite data retains registrations and publications. A peer that changes its address registers again with the same ID.
- A source publishes a complete local file and keeps its path and contents stable. Mirrors use the same chunk size to share a manifest ID. A source may be added while a download is in progress. A client needs the directory for initial manifest lookup and consults it again after exhausting a source pass.
- A job is identified locally by its output path and manifest ID. Only one downloader may write a job at once. Successful assembly atomically replaces any existing output at that path. A sidecar for a different manifest prevents an accidental resume into the wrong job. The filesystem must support POSIX advisory locking, atomic rename, and directory fsync.
- A timeout or a failed hash retries a complete chunk on an alternative peer when one is listed. A download can finish if at least one reachable peer can supply each remaining chunk within the retry budget. If sources are exhausted, the command fails with verified progress still available for a later invocation.
- The experiment throttles each request independently. Its byte counts measure payload received and recorded by the downloader. A hard kill of the downloader itself can leave an in-flight attempt unrecorded. The restart test checks checkpoint reuse and request ID continuity. The source-kill experiment keeps the downloader alive to account for interrupted payload.
