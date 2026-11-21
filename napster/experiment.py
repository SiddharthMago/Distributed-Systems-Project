"""Compare baseline and source loss in a six-peer deployment."""

import argparse
import csv
import json
import platform
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean

from .__main__ import positive
from .download import TransferError, state_dir, status
from .lab import Lab, make_payload, stop, wait_for
from .model import MIB, sha256_file

FIELDS = (
    "trial", "scenario", "elapsed_seconds", "retransmitted_bytes",
    "hash_equal", "verified_at_disconnect", "payload_bytes", "commits",
)


def run_trial(root, fixture, trial, scenario):
    with Lab(root) as lab:
        lab.start_directory()
        for index in range(1, 7):
            name = f"peer{index}"
            lab.start_peer(name, rate=4)
            note = root / name / f"{name}.txt"
            note.write_text(f"Shared by {name}\n")
            lab.publish(name, note)
        file_ids = []
        for name in ("peer1", "peer2"):
            seed = root / name / "sample.bin"
            shutil.copyfile(fixture, seed)
            file_ids.append(lab.publish(name, seed))
        if len(set(file_ids)) != 1:
            raise RuntimeError("Seed manifests differ")
        output = root / "peer6" / "received.bin"
        process = lab.downloader("download", file_ids[0], output, "--workers", 4)
        committed_before_drop = set()
        injection = None
        if scenario == "disconnect":
            halfway = (fixture.stat().st_size + 2 * MIB - 1) // (2 * MIB)

            def ready_to_drop():
                if process.poll() is not None:
                    raise RuntimeError("Downloader exited before failure injection")
                try:
                    current = status(output, details=True)
                except TransferError:
                    return None
                if current["verified_chunks"] < halfway:
                    return None
                active = [item for item in lab.active("peer1")
                          if item["file_id"] == file_ids[0]
                          and 0 < item["bytes_written"] < item["length"]]
                return (current, active) if active else None

            current, active = wait_for(ready_to_drop, timeout=120, interval=0.01)
            stop(lab.peers["peer1"][0], hard=True)
            committed_before_drop = {item["idx"] for item in current["chunks"] if item["complete"]}
            injection = {"peer": "peer1", "verified_chunks": current["verified_chunks"],
                         "active_at_disconnect": active}

        result = lab.result("download", process, timeout=240)
        full = status(output, details=True)
        expected = sha256_file(fixture)
        if sha256_file(output) != expected or result["actual_sha256"] != expected:
            raise RuntimeError("Final hash differs from original fixture")
        if any(chunk["commits"] != 1 for chunk in full["chunks"]):
            raise RuntimeError("A chunk was skipped or committed more than once")

        attempts = defaultdict(list)
        for attempt in full["attempts"]:
            attempts[attempt["idx"]].append(attempt)
        if any(len(attempts[index]) != 1 for index in committed_before_drop):
            raise RuntimeError("A previously committed chunk was requested again")
        if scenario == "disconnect":
            recovered = [index for index, rows in attempts.items()
                         if any(row["peer"] == "peer1" and 0 < row["bytes"] < MIB
                                and row["outcome"] != "verified" for row in rows)
                         and rows[-1]["peer"] == "peer2" and rows[-1]["outcome"] == "verified"]
            if not recovered or not result["retransmitted_bytes"]:
                raise RuntimeError("The experiment did not capture interrupted payload and recovery")
            injection["partial_chunks_retried_on_peer2"] = sorted(recovered)
            injection["previously_committed_chunk_requests_after_failure"] = 0
        elif result["retransmitted_bytes"] != 0:
            raise RuntimeError("The baseline unexpectedly retransmitted payload")

        row = {key: result[key] for key in FIELDS if key in result}
        row.update(trial=trial, scenario=scenario,
                   verified_at_disconnect=injection["verified_chunks"] if injection else None)
        return {**row, "expected_sha256": expected, "actual_sha256": result["actual_sha256"],
                "peers": result["peers"], "injection": injection}


def write_results(destination, rows, size):
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    destination.mkdir(parents=True, exist_ok=True)
    host = f"{platform.platform()}, Python {platform.python_version()}"
    evidence = {"host": host, "size_bytes": size, "chunk_size_bytes": MIB,
                "workers": 4, "rate_mib_per_request": 4, "peers": 6, "runs": rows}
    (destination / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    with (destination / "measurements.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    fig, axes = plt.subplots(1, 2, figsize=(9, 3.8), layout="constrained")
    report = [
        "# Source-disconnection experiment", "",
        f"Host **{host}**. File **{size // MIB} MiB**, with 1 MiB chunks, four workers, "
        "and a 4 MiB/s cap per request. Six peers ran alongside one persistent directory. "
        "Peer 1 and peer 2 published identical full copies, and peer 6 was the downloader.", "",
        "| Trial | Scenario | Completion (s) | Repeated payload (bytes) | Verified at kill | Hash equal |",
        "| ---: | --- | ---: | ---: | ---: | :---: |",
    ]
    for row in rows:
        verified = row["verified_at_disconnect"]
        report.append(f"| {row['trial']} | {row['scenario']} | {row['elapsed_seconds']:.3f} | "
                      f"{row['retransmitted_bytes']:,} | {verified if verified is not None else '—'} | "
                      f"{'yes' if row['hash_equal'] else 'no'} |")
    report += ["", "| Scenario | Mean completion (s) | Mean repeated payload (bytes) |",
               "| --- | ---: | ---: |"]
    metrics = (("elapsed_seconds", 1, "Completion time (s)"),
               ("retransmitted_bytes", 1024, "Repeated payload (KiB)"))
    for group, label in enumerate(("baseline", "disconnect")):
        runs = [row for row in rows if row["scenario"] == label]
        elapsed = mean(run["elapsed_seconds"] for run in runs)
        repeated = mean(run["retransmitted_bytes"] for run in runs)
        report.append(f"| {label} | {elapsed:.3f} | {repeated:,.1f} |")
        for axis, (key, divisor, title) in zip(axes, metrics):
            values = [run[key] / divisor for run in runs]
            color = ("#2C688E", "#AE4B22")[group]
            axis.bar(group, mean(values), color=color, alpha=0.78)
            offsets = [0.18 * (i / max(1, len(values) - 1) - 0.5) for i in range(len(values))]
            axis.scatter([group + offset for offset in offsets], values, color="#242424", s=17)
            axis.set_xticks([0, 1], ["Baseline", "Source stopped"])
            axis.set_ylabel(title)
    for axis, (key, divisor, _) in zip(axes, metrics):
        maximum = max(run[key] / divisor for run in rows)
        axis.set_ylim(0, (maximum or 1) * 1.12)
    fig.savefig(destination / "comparison.png", dpi=160)
    plt.close(fig)
    report += [
        "", "![Per-trial observations and scenario means](comparison.png)", "",
        f"Expected and observed SHA-256 in every trial: `{rows[0]['expected_sha256']}`.", "",
        "The driver stopped peer 1 with SIGKILL after polling past the halfway checkpoint "
        "and observing an active chunk with a partial body. Every stopped-source run recorded "
        "at least one partial attempt on peer 1 followed by verification of that chunk on peer 2. "
        "Every run committed each chunk exactly once. No chunks already committed at the "
        "fault checkpoint were requested again.", "",
        "Completion time includes manifest discovery, checkpoint writes, assembly and final verification. "
        "Repeated bytes count overlapping payload offsets received by the downloader across attempts "
        "for the same chunk. Three trials on one loopback host describe this setup. "
        "The four workers could all continue against peer 2 after the kill because the cap was "
        "per request and the server could handle concurrent requests.", "",
        "See [measurements.csv](measurements.csv) for unrounded numbers and "
        "[evidence.json](evidence.json) for peer attribution and the observed fault state.", "",
    ]
    (destination / "REPORT.md").write_text("\n".join(report))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=positive, default=100,
                        help="Fixture size in MiB (minimum 8, default 100)")
    parser.add_argument("--trials", type=positive, default=3)
    parser.add_argument("--work-dir", type=Path, default=Path("run/experiment"))
    parser.add_argument("--results", type=Path, default=Path("run/results"))
    args = parser.parse_args()
    if args.size_mib < 8:
        parser.error("Use at least 8 MiB so there is active work after the halfway checkpoint.")
    root = args.work_dir.resolve()
    if root.exists():
        parser.error(f"Work directory already exists: {root}. Choose a fresh path.")
    fixture = root / "fixture.bin"
    size = args.size_mib * MIB
    make_payload(fixture, size)
    rows = []
    for trial in range(1, args.trials + 1):
        for scenario in ("baseline", "disconnect"):
            trial_dir = root / f"{trial}-{scenario}"
            row = run_trial(trial_dir, fixture, trial, scenario)
            rows.append(row)
            print(json.dumps(row), flush=True)
            for file in (trial_dir / "peer1/sample.bin", trial_dir / "peer2/sample.bin",
                         trial_dir / "peer6/received.bin"):
                file.unlink()
            for file in state_dir(trial_dir / "peer6/received.bin").glob("*.chunk"):
                file.unlink()
    write_results(args.results, rows, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
