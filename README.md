# Napster with Reliable File Transfers

Distributed Systems course project · Ananth Rajesh, Mithun Rameshbabu, and Siddharth Mago

A SQLite-backed directory indexes file manifests and peer addresses. Six peers can publish files and stream chunks directly to downloaders. The downloader uses four concurrent workers by default, verifies each chunk with SHA-256, and uses its on-disk checkpoint to resume after a restart.

See [architecture and protocol](PROTOCOL.md) for message formats, retry behavior, and crash recovery. The [experiment report](results/REPORT.md) includes all six measured runs, with the [CSV](results/measurements.csv) and [fault evidence](results/evidence.json) alongside it.

## Setup

Use Python 3.12 on macOS or Linux. From the project root, create a virtual environment and install the project and analysis dependencies.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Use `requirements.txt` for the directory, peer, and client CLI only. The tests and local experiment bind temporary loopback ports. A four-core machine with 8 GB of RAM and 10 GB of free disk space is a planning estimate. The default experiment needs roughly 500 MiB for payload files at peak, plus logs, SQLite files, and Python dependencies. A 1 GiB trial needs roughly 5 GiB for payload files at peak. An RCE account is unnecessary for the local procedure.

## Reproduce the failure experiment

```sh
python -m napster.experiment
```

The command launches one directory plus six peer processes for each run, publishes a deterministic 100 MiB file on peers 1 and 2, and downloads into peer 6's state directory. It runs three baselines and three peer-loss trials. In each peer-loss trial it stops peer 1 with SIGKILL once at least half the file is verified and peer 1 has an active request with a partially written body. It checks that the unfinished chunk was retried on peer 2, previously committed chunks were requested only once, each chunk was committed once, and the final hash equals the original fixture hash. The work directory retains process logs and SQLite evidence. The driver removes completed trial payload copies after verifying them to limit disk use.

Outputs go to `run/results/` with logs under `run/experiment/`. Use a fresh work directory each time. For a shorter demonstration, use the following settings.

```sh
python -m napster.experiment --size-mib 16 --trials 1 \
  --work-dir run/quick-demo --results run/quick-results
```

## Launch the services yourself

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
