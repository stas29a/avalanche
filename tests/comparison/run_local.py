#!/usr/bin/env python3
"""Build and compare native Avalanche versions over loopback TCP, without Docker."""

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
import csv
import hashlib
import html
import importlib.util
import itertools
import json
import math
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RESULTS = HERE / "results" / "local"
FORMAT = "avalanche-native-v1"


def available_cpu_count():
    if hasattr(os, "sched_getaffinity"):
        try:
            return max(1, len(os.sched_getaffinity(0)))
        except OSError:
            pass
    return getattr(os, "process_cpu_count", os.cpu_count)() or 1


DEFAULTS = {
    "clients": [1, 8, 32, 128], "modes": ["get", "set", "mixed"],
    "seconds": 2.0, "warmup_seconds": 0.5, "repeats": 3,
    "keys": 1024, "value_bytes": 256, "server_threads": available_cpu_count(), "client_threads": 4,
}
SOURCE_PATHS = ["src", "tests/comparison/src", "tests/comparison/Cargo.toml", "tests/comparison/Cargo.lock"]
ROOT_BUILD_PATHS = ["Cargo.toml", "Cargo.lock"]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def capture(command, cwd=ROOT):
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(f"{' '.join(map(str, command))}: {result.stderr.strip()}")
    return result.stdout.strip()


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    baseline = parser.add_mutually_exclusive_group()
    baseline.add_argument("--baseline", type=Path, help="rerun the saved current-server binary from this local run")
    baseline.add_argument("--baseline-ref", help="build the baseline from a Git revision, e.g. HEAD or HEAD~1")
    baseline.add_argument("--no-compare", action="store_true", help="measure current code only and save it as a future baseline")
    parser.add_argument("--current-ref", help="build current from this Git revision instead of the working tree")
    parser.add_argument("--output", type=Path, help="new result directory; must not exist")
    parser.add_argument("--quick", action="store_true", help="smoke preset: 1/8 clients, 0.2s, no warmup, one repeat")
    parser.add_argument("--no-plots", action="store_true", help="write CSV/HTML without optional matplotlib charts")
    parser.add_argument("--clients", help="comma-separated connection counts")
    parser.add_argument("--modes", "--mode", dest="modes", help="comma-separated get,set,mixed")
    for name in ["seconds", "warmup-seconds"]:
        parser.add_argument(f"--{name}", type=float)
    for name in ["repeats", "keys", "value-bytes", "server-threads", "client-threads"]:
        help_text = "server worker threads; defaults to all available logical CPUs" if name == "server-threads" else None
        parser.add_argument(f"--{name}", type=int, help=help_text)
    args = parser.parse_args(argv)
    defaults = {**DEFAULTS}
    if args.quick:
        defaults.update(clients=[1, 8], seconds=0.2, warmup_seconds=0, repeats=1)
    try:
        args.clients = [int(n) for n in args.clients.split(",")] if args.clients else defaults["clients"]
        args.modes = args.modes.split(",") if args.modes else defaults["modes"]
    except ValueError:
        parser.error("--clients must contain comma-separated integers")
    for key in DEFAULTS:
        if getattr(args, key) is None:
            setattr(args, key, defaults[key])
    if not args.clients or len(set(args.clients)) != len(args.clients) or min(args.clients) < 1:
        parser.error("--clients requires distinct positive integers")
    if not args.modes or len(set(args.modes)) != len(args.modes) or set(args.modes) - {"get", "set", "mixed"}:
        parser.error("--modes requires distinct names from get,set,mixed")
    if not math.isfinite(args.seconds) or not 0 < args.seconds <= 3600:
        parser.error("--seconds must be finite and in (0, 3600]")
    if not math.isfinite(args.warmup_seconds) or not 0 <= args.warmup_seconds <= 3600:
        parser.error("--warmup-seconds must be finite and in [0, 3600]")
    if min(args.repeats, args.keys, args.server_threads, args.client_threads) < 1:
        parser.error("repeats, keys, and thread counts must be positive")
    if args.keys > 2**32 - 1 or not 0 <= args.value_bytes <= 16 * 1024 * 1024:
        parser.error("keys must fit u32; value size must be between 0 and 16 MiB")
    return args


def previous_run(environment, directory=RESULTS):
    candidates = []
    for path in directory.glob("*/metadata.json"):
        try:
            data = json.loads(path.read_text())
            if (data.get("format") == FORMAT and data.get("status") == "success"
                    and data["environment"]["target"] == environment["target"]):
                candidates.append((data["finished_utc"], path.parent))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return max(candidates)[1] if candidates else None


@contextmanager
def exclusive_run():
    import fcntl
    lock = HERE / "target" / "local-run.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another local benchmark is running; wait for it to finish") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def saved_baseline(directory, environment):
    directory = directory.resolve()
    data = json.loads((directory / "metadata.json").read_text())
    if data.get("format") != FORMAT or data.get("status") != "success":
        raise ValueError("--baseline requires a successful native local run, not Docker or standalone load-test results")
    if data["environment"]["target"] != environment["target"]:
        raise ValueError("baseline target differs from the native host target")
    binary = directory / "bin" / "current-server"
    provenance = data["variants"]["current"]
    if sha256(binary) != provenance["server_sha256"]:
        raise ValueError("saved baseline binary changed; refusing an unverifiable comparison")
    return binary, {**provenance, "origin": "saved local run", "run": str(directory)}


def snapshot_current(destination):
    for name in SOURCE_PATHS + ROOT_BUILD_PATHS:
        source = ROOT / name
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)


def snapshot_revision(revision, destination):
    destination.mkdir(parents=True)
    archive = destination.parent / "baseline.tar"
    try:
        with archive.open("wb") as stream:
            # Older revisions may not have a tracked root lockfile.
            root_paths = capture(["git", "ls-tree", "--name-only", revision, "--", *ROOT_BUILD_PATHS]).splitlines()
            result = subprocess.run(["git", "archive", "--format=tar", revision, *SOURCE_PATHS, *root_paths],
                                    cwd=ROOT, stdout=stream, stderr=subprocess.PIPE, check=False)
        if result.returncode:
            raise RuntimeError("Git revision must contain src/ and the comparison fixture: "
                               + result.stderr.decode(errors="replace").strip())
        with tarfile.open(archive) as stream:
            stream.extractall(destination, filter="data")
    finally:
        archive.unlink(missing_ok=True)


def source_hashes(directory):
    return {str(p.relative_to(directory)): sha256(p)
            for p in sorted(directory.rglob("*")) if p.is_file()}


def build(source, variant, output, target, binaries=("bench-server",)):
    print(f"Building {variant} (release, native {target})...", flush=True)
    log_path = output / "logs" / f"build-{variant}.log"
    packages = {}
    for name in binaries:
        # Historical snapshots still contain the standalone server fixture.
        root_server = name == "bench-server" and (source / "src/bin/bench-server.rs").is_file()
        manifest = source / ("Cargo.toml" if root_server else "tests/comparison/Cargo.toml")
        package = "avalanche" if root_server else "avalanche-comparison"
        packages.setdefault((manifest, package), []).append(name)
    with log_path.open("w") as stream:
        for (manifest, package), names in packages.items():
            build_options = ["--release", "--target", target,
                             "--manifest-path", str(manifest),
                             "--target-dir", str(HERE / "target" / "local")]
            # Prevent cached dep-info from referring to a previous snapshot.
            clean = ["cargo", "clean", "--package", package, *build_options]
            result = subprocess.run(clean, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                raise RuntimeError(f"{variant} cache cleanup failed; see {log_path}")
            command = ["cargo", "build", "--locked", *build_options]
            for name in names:
                command += ["--bin", name]
            result = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                raise RuntimeError(f"{variant} build failed; see {log_path}")
    binary_dir = HERE / "target" / "local" / target / "release"
    for name in binaries:
        destination = "bench-client" if name == "bench-client" else f"{variant}-server"
        shutil.copy2(binary_dir / name, output / "bin" / destination)


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def wait_ready(process, address):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup (status {process.returncode})")
        try:
            with socket.create_connection(address, timeout=0.2):
                pass
            try:
                process.wait(timeout=0.02)
            except subprocess.TimeoutExpired:
                return
            raise RuntimeError(f"server exited during startup (status {process.returncode})")
        except OSError:
            time.sleep(0.02)
    raise RuntimeError("server did not become ready within 10 seconds")


@contextmanager
def running_server(binary, address, threads, log_path):
    environment = {**os.environ, "BENCH_SERVER_ADDR": f"{address[0]}:{address[1]}",
                   "BENCH_SERVER_THREADS": str(threads), "BENCH_SERVER_WORKERS": "30"}
    with log_path.open("w") as log:
        process = subprocess.Popen([str(binary)], cwd=ROOT, env=environment,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            wait_ready(process, address)
            yield process
        finally:
            stop_process(process)


def validate_samples(rows, settings, modes=None, clients=None):
    expected = Counter((mode, count, repeat)
                       for mode in modes or settings["modes"]
                       for count in clients or settings["clients"]
                       for repeat in range(1, settings["repeats"] + 1))
    actual = Counter((r["mode"], r["clients"], r["repeat"]) for r in rows)
    if actual != expected:
        raise ValueError("incomplete or duplicate measurement grid")
    for row in rows:
        if (row["database"] != "avalanche" or row["errors"] != 0 or row["completed"] <= 0
                or row["completed"] != row["get"] + row["set"]):
            raise ValueError("failed or invalid measured responses")
        if ((row["mode"] == "get" and row["set"] != 0)
                or (row["mode"] == "set" and row["get"] != 0)
                or (row["mode"] == "mixed" and abs(row["get"] - row["set"]) > row["clients"])):
            raise ValueError("operation counts differ from requested mode")
        if any(row[key] != settings[key] for key in ["seconds", "keys", "value_bytes"]):
            raise ValueError("sample settings differ from requested workload")
        if not math.isfinite(row["rps"]) or not math.isclose(
                row["rps"], row["completed"] / row["seconds"], rel_tol=0, abs_tol=0.001):
            raise ValueError("invalid RPS calculation")
        if (any(not math.isfinite(row[key]) for key in ["p50_us", "p95_us", "p99_us"])
                or not 0 < row["p50_us"] <= row["p95_us"] <= row["p99_us"]):
            raise ValueError("invalid latency percentiles")


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row["mode"], row["clients"]].append(row)
    return [{"mode": mode, "clients": clients, "repeats": len(samples),
             "rps_median": statistics.median(s["rps"] for s in samples),
             "rps_min": min(s["rps"] for s in samples), "rps_max": max(s["rps"] for s in samples),
             **{f"{metric}_median": statistics.median(s[metric] for s in samples)
                for metric in ["p50_us", "p95_us", "p99_us"]}}
            for (mode, clients), samples in sorted(groups.items())]


def compare(baseline, current):
    previous = {(r["mode"], r["clients"]): r for r in baseline}
    if set(previous) != {(r["mode"], r["clients"]) for r in current}:
        raise ValueError("baseline and current measurement groups differ")
    result = []
    for new in current:
        old = previous[new["mode"], new["clients"]]
        result.append({"mode": new["mode"], "clients": new["clients"],
                       **{f"baseline_{key}": value for key, value in old.items() if key not in ["mode", "clients"]},
                       **{f"current_{key}": value for key, value in new.items() if key not in ["mode", "clients"]},
                       "rps_change_percent": (new["rps_median"] / old["rps_median"] - 1) * 100,
                       "p99_change_percent": (new["p99_us_median"] / old["p99_us_median"] - 1) * 100})
    return result


def measure(output, settings, variants, metadata):
    rows = {variant: [] for variant in variants}
    for variant in variants:
        (output / variant).mkdir()
    for index, (mode, clients) in enumerate(itertools.product(settings["modes"], settings["clients"])):
        # Alternate which version goes first; each point starts with a fresh server.
        order = variants if index % 2 == 0 else list(reversed(variants))
        for variant in order:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                address = reservation.getsockname()
            stem = f"{variant}-{mode}-{clients}"
            server_log = output / "logs" / f"{stem}-server.log"
            client_log = output / "logs" / f"{stem}-client.log"
            print(f"{variant:8s} {mode:5s} clients={clients:3d} ({settings['repeats']} repeats)", flush=True)
            with running_server(output / "bin" / f"{variant}-server", address,
                                settings["server_threads"], server_log) as server:
                command = [str(output / "bin" / "bench-client"), "--database", "avalanche",
                           "--address", f"{address[0]}:{address[1]}", "--mode", mode,
                           "--clients", str(clients), "--threads", str(settings["client_threads"])]
                for key in ["seconds", "warmup_seconds", "repeats", "keys", "value_bytes"]:
                    command += ["--" + key.replace("_", "-"), str(settings[key])]
                timeout = max(60, settings["repeats"] * (settings["seconds"] + settings["warmup_seconds"] + 5)
                              + 30 + settings["keys"] / 100)
                with client_log.open("w") as log:
                    result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                            timeout=timeout, check=False)
                if result.returncode or server.poll() is not None:
                    raise RuntimeError(f"{stem} failed; see {client_log} and {server_log}")
                lines = client_log.read_text().splitlines()
                protocols = [line.partition("=")[2] for line in lines
                             if line.startswith("AVALANCHE_RESPONSE_FORMAT=")]
                if len(protocols) != 1:
                    raise ValueError(f"{stem}: missing or ambiguous response-format check")
                previous_protocol = metadata["variants"][variant].get("response_protocol")
                if previous_protocol and previous_protocol != protocols[0]:
                    raise ValueError(f"{variant}: response protocol changed between measurement points")
                metadata["variants"][variant]["response_protocol"] = protocols[0]
                samples = [json.loads(line) for line in lines if line.startswith("{")]
                validate_samples(samples, settings, [mode], [clients])
            rows[variant].extend(samples)
            with (output / variant / "raw.jsonl").open("a") as stream:
                for row in samples:
                    stream.write(json.dumps(row) + "\n")
            metadata["sample_counts"][variant] = len(rows[variant])
            save_json(output / "metadata.json", metadata)
            print(f"  RPS {statistics.median(r['rps'] for r in samples):,.0f}; errors 0", flush=True)
    summaries = {}
    for variant, samples in rows.items():
        validate_samples(samples, settings)
        write_csv(output / variant / "raw.csv", samples)
        summaries[variant] = summarize(samples)
        write_csv(output / variant / "summary.csv", summaries[variant])
    return summaries


def charts(output, summaries, settings):
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "avalanche-local-matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "avalanche-local-cache"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, len(settings["modes"]), figsize=(6 * len(settings["modes"]), 8), squeeze=False)
    for column, mode in enumerate(settings["modes"]):
        for variant, data in summaries.items():
            points = sorted((r for r in data if r["mode"] == mode), key=lambda r: r["clients"])
            x = [p["clients"] for p in points]
            color = "#2563eb" if variant == "current" else "#64748b"
            axes[0, column].plot(x, [p["rps_median"] / 1000 for p in points], "o-", label=variant, color=color)
            axes[0, column].fill_between(x, [p["rps_min"] / 1000 for p in points],
                                        [p["rps_max"] / 1000 for p in points], color=color, alpha=0.12)
            axes[1, column].plot(x, [p["p99_us_median"] for p in points], "o-", label=variant, color=color)
        for axis in axes[:, column]:
            axis.set_xscale("log", base=2)
            axis.set_xticks(sorted(settings["clients"]), labels=sorted(settings["clients"]))
            axis.set_xlabel("Concurrent TCP connections (log scale)")
            axis.set_ylim(bottom=0)
            axis.grid(alpha=0.2)
            axis.legend()
        axes[0, column].set_title(mode.upper() if mode != "mixed" else "50% GET / 50% SET")
        axes[0, column].set_ylabel("Thousand validated requests / second")
        axes[1, column].set_ylabel("Median per-run p99 (microseconds)")
    fig.suptitle("Avalanche: native local benchmark", fontsize=18)
    fig.text(0.5, 0.02, f"Loopback; pipeline 1; {settings['value_bytes']} B; {settings['keys']:,} keys; "
             f"{settings['server_threads']} server / {settings['client_threads']} client threads\n"
             f"{settings['repeats']} x {settings['seconds']:g}s; warmup {settings['warmup_seconds']:g}s; "
             "RPS shading: min-max. Shared host scheduler; no CPU pinning.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.08, 1, 0.95))
    for extension in ["png", "svg"]:
        fig.savefig(output / f"comparison.{extension}", dpi=160, facecolor="white")
    plt.close(fig)


def report(output, summaries, changes, metadata, plots):
    print("\nMODE   CLIENTS  BASELINE RPS  CURRENT RPS   CHANGE   CURRENT p99 (us)")
    table = []
    changes_by_key = {(r["mode"], r["clients"]): r for r in changes}
    for row in summaries["current"]:
        change = changes_by_key.get((row["mode"], row["clients"]))
        before = f"{change['baseline_rps_median']:,.0f}" if change else "-"
        delta = f"{change['rps_change_percent']:+.1f}%" if change else "-"
        old_latency = str(change["baseline_p99_us_median"]) if change else "-"
        print(f"{row['mode']:5s} {row['clients']:8d} {before:>13s} {row['rps_median']:12,.0f} "
              f"{delta:>8s} {row['p99_us_median']:18g}")
        table.append(f"<tr><td>{row['mode']}</td><td>{row['clients']}</td><td>{before}</td>"
                     f"<td>{row['rps_median']:,.0f}</td><td>{delta}</td>"
                     f"<td>{old_latency}</td><td>{row['p99_us_median']:g}</td></tr>")
    image = '<img src="comparison.png" alt="Native baseline/current RPS and p99">' if plots else ""
    links = " ".join(f'<a href="{v}/raw.csv">{v} raw CSV</a> <a href="{v}/summary.csv">{v} summary</a>'
                     for v in summaries)
    page = f'''<!doctype html><html lang="en"><meta charset="utf-8"><title>Avalanche local comparison</title>
<style>body{{font:16px system-ui;max-width:1400px;margin:40px auto;padding:0 24px;color:#172033}}
img{{width:100%}}table{{border-collapse:collapse;width:100%}}td,th{{padding:10px;border-bottom:1px solid #ddd;text-align:left}}pre{{white-space:pre-wrap}}</style>
<h1>Avalanche native local benchmark</h1><p>{html.escape(metadata['started_utc'])}</p>
<p>Separate server and client processes on loopback TCP. Pipeline 1; complete responses validated.
Each point uses a fresh server. Baseline/current order alternates between points. Both versions use
the current load generator and identical workload settings. No Docker, CPU pinning, or memory quotas.</p>
{image}<table><tr><th>Mode</th><th>Clients</th><th>Baseline RPS</th><th>Current RPS</th><th>RPS change</th>
<th>Baseline p99, us</th><th>Current p99, us</th></tr>{''.join(table)}</table>
<p>RPS: median of repeats; positive change means higher throughput. p99: median of per-run percentiles,
not a pooled percentile. Chart bands show repeat min/max. Read small differences alongside the ranges in summary CSV.</p>
<p>{links} <a href="metadata.json">Metadata and source hashes</a></p>
<h2>Settings and provenance</h2><pre>{html.escape(json.dumps(metadata, indent=2))}</pre></html>'''
    (output / "report.html").write_text(page)


def execute(args):
    if sys.version_info < (3, 12):
        raise RuntimeError("Python 3.12 or newer is required")
    if platform.system() not in ["Darwin", "Linux"]:
        raise RuntimeError("the local runner supports macOS and Linux")
    rustc = capture(["rustc", "-Vv"])
    environment = {"system": platform.system(), "machine": platform.machine(), "platform": platform.platform(),
                   "logical_cpus": os.cpu_count(), "target": next(line[6:] for line in rustc.splitlines() if line.startswith("host: "))}
    baseline_directory = args.baseline
    if not baseline_directory and not args.baseline_ref and not args.no_compare:
        baseline_directory = previous_run(environment)
    saved = saved_baseline(baseline_directory, environment) if baseline_directory else None
    revision = (capture(["git", "rev-parse", "--verify", f"{args.baseline_ref}^{{commit}}"])
                if args.baseline_ref else None)
    current_revision = (capture(["git", "rev-parse", "--verify", f"{args.current_ref}^{{commit}}"])
                        if args.current_ref else None)
    settings = {key: getattr(args, key) for key in DEFAULTS}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or RESULTS / f"{stamp}-{uuid.uuid4().hex[:8]}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "bin").mkdir()
    (output / "logs").mkdir()
    metadata = {"format": FORMAT, "status": "running", "started_utc": utc_now(), "environment": environment,
                "settings": settings, "variants": {}, "sample_counts": {}, "method": {
                    "pipeline": 1, "transport": "native loopback TCP", "cpu_affinity": "none",
                    "request_logging": "disabled by benchmark fixture",
                    "client": "current generator for both versions; byte-validated responses inside fixed windows",
                    "order": "alternating baseline/current order per mode/client point; fresh server for each point"}}
    save_json(output / "metadata.json", metadata)
    print(f"Results: {output}", flush=True)
    try:
        current_source = output / "sources" / "current"
        if current_revision:
            snapshot_revision(current_revision, current_source)
        else:
            snapshot_current(current_source)
        current_provenance = {"origin": "Git revision" if current_revision else "working tree snapshot",
                              "git_revision": current_revision or capture(["git", "rev-parse", "HEAD"]),
                              "source_sha256": source_hashes(current_source), "rustc": rustc,
                              "cargo": capture(["cargo", "-V"]), "rustflags": os.environ.get("RUSTFLAGS", "")}
        binaries = ("bench-server",) if current_revision else ("bench-server", "bench-client")
        build(current_source, "current", output, environment["target"], binaries)
        metadata["variants"]["current"] = {**current_provenance, "server_sha256": sha256(output / "bin/current-server")}
        client_source = current_source
        if current_revision:
            client_source = output / "sources" / "client"
            snapshot_current(client_source)
            build(client_source, "client", output, environment["target"], ("bench-client",))
        metadata["client_sha256"] = sha256(output / "bin/bench-client")
        metadata["client_source_sha256"] = {name: sha256(client_source / name) for name in [
            "tests/comparison/Cargo.toml", "tests/comparison/Cargo.lock",
            "tests/comparison/src/lib.rs", "tests/comparison/src/bin/client.rs"]}
        if saved:
            binary, provenance = saved
            shutil.copy2(binary, output / "bin/baseline-server")
            if sha256(output / "bin/baseline-server") != provenance["server_sha256"]:
                raise RuntimeError("saved baseline changed while it was being copied")
            metadata["variants"]["baseline"] = provenance
            print(f"Baseline: saved binary from {baseline_directory}", flush=True)
        elif revision:
            source = output / "sources" / "baseline"
            snapshot_revision(revision, source)
            provenance = {"origin": "Git revision", "git_revision": revision, "requested_ref": args.baseline_ref,
                          "source_sha256": source_hashes(source), "rustc": rustc,
                          "cargo": current_provenance["cargo"], "rustflags": current_provenance["rustflags"]}
            build(source, "baseline", output, environment["target"])
            metadata["variants"]["baseline"] = {**provenance, "server_sha256": sha256(output / "bin/baseline-server")}
            print(f"Baseline: {args.baseline_ref} ({revision[:12]})", flush=True)
        else:
            print("No baseline selected/found. Saving current version for the next local comparison.", flush=True)
        save_json(output / "metadata.json", metadata)
        variants = ["baseline", "current"] if "baseline" in metadata["variants"] else ["current"]
        summaries = measure(output, settings, variants, metadata)
        changes = compare(summaries["baseline"], summaries["current"]) if "baseline" in summaries else []
        if changes:
            write_csv(output / "comparison.csv", changes)
        plots = not args.no_plots and importlib.util.find_spec("matplotlib") is not None
        if plots:
            charts(output, summaries, settings)
        metadata.update(status="success", finished_utc=utc_now(), errors=0,
                        validated_responses={v: sum(json.loads(line)["completed"] for line in
                                                   (output / v / "raw.jsonl").read_text().splitlines()) for v in variants},
                        plots=plots)
        report(output, summaries, changes, metadata, plots)
        save_json(output / "metadata.json", metadata)
        print(f"\nReport: {output / 'report.html'}", flush=True)
        return 0
    except BaseException as error:
        metadata.update(status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                        finished_utc=utc_now(), error=str(error) or type(error).__name__)
        save_json(output / "metadata.json", metadata)
        if isinstance(error, KeyboardInterrupt):
            print(f"Interrupted. Owned server processes stopped; partial results: {output}", file=sys.stderr)
            return 130
        raise


def main(argv=None):
    args = arguments(argv)
    with exclusive_run():
        return execute(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
