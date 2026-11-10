#!/usr/bin/env python3
"""Run isolated, sequential database benchmarks and preserve every raw sample."""
import argparse
from collections import Counter
import copy
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import sys
import time
import uuid
from urllib.parse import unquote, urlparse

from memory import METHOD as MEMORY_METHOD, MemorySampler

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CLIENT_IMAGE = "avalanche-comparison:local"
PARAMETERS = {
    "clients": [1, 8, 32, 128], "seconds": 2, "warmup_seconds": 0.5,
    "repeats": 3, "keys": 1024, "value_bytes": 256,
    "server_cpuset": "0-3", "client_cpuset": "4-7",
    "server_threads": 4, "client_threads": 4,
    "server_memory": "2g", "database_memory_mib": 1024, "network_mode": "bridge",
}
REPORT_FILES = ["metadata.json", "raw.jsonl", "raw.csv", "summary.csv", "best.csv", "report.html"] + [
    f"{name}.{extension}" for name in ["throughput", "best_rps", "latency_p99", "memory"] for extension in ["png", "svg"]]


def snapshot(directory):
    return {name: (directory / name).read_bytes() for name in ["metadata.json", "raw.jsonl"]}


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        # Updates may combine legacy rows without RAM and new measurements.
        writer = csv.DictWriter(stream, fieldnames=list(dict.fromkeys(key for row in rows for key in row)))
        writer.writeheader()
        writer.writerows(rows)


def merge_update(directory, run_directory, original):
    """Validate and render first; archive replaced files and roll back on write failure."""
    if snapshot(directory) != original:
        raise RuntimeError("existing results changed during this run; update was not applied")
    previous = json.loads(original["metadata.json"])
    current = json.loads((run_directory / "metadata.json").read_text())
    selected = current["settings"]["databases"]
    if current.get("failures"):
        raise ValueError("failed measurements cannot replace existing results")
    for key in PARAMETERS:
        if current["settings"].get(key, PARAMETERS[key]) != previous["settings"].get(key, PARAMETERS[key]):
            raise ValueError(f"cannot update results with different {key}")
    if current["client_image"]["Architecture"] != previous["client_image"]["Architecture"]:
        raise ValueError("cannot update results measured on a different architecture")
    rows = [json.loads(line) for line in (run_directory / "raw.jsonl").read_text().splitlines() if line]
    expected = Counter((database, mode, clients, repeat)
                       for database in selected for mode in ["get", "set", "mixed"]
                       for clients in current["settings"]["clients"]
                       for repeat in range(1, current["settings"]["repeats"] + 1))
    actual = Counter((row["database"], row["mode"], row["clients"], row["repeat"]) for row in rows)
    if actual != expected or any(row["errors"] for row in rows):
        raise ValueError("update requires a complete set of successful measurements")
    for row in rows:
        if any(row[key] != current["settings"][key] for key in ["seconds", "keys", "value_bytes"]):
            raise ValueError("sample parameters differ from run settings")
    retained = [line for line in original["raw.jsonl"].decode().splitlines()
                if line and json.loads(line)["database"] not in selected]
    combined = "\n".join(retained + [json.dumps(row) for row in rows]) + "\n"
    merged = copy.deepcopy(previous)
    relative_run = str(run_directory.relative_to(directory))
    for database in selected:
        merged["databases"][database] = {
            **current["databases"][database], "measurement_run": relative_run,
            "measured_utc": current["finished_utc"],
            "source_sha256": current["source_sha256"], "client_image": current["client_image"],
        }
    merged["failures"] = [failure for failure in previous.get("failures", [])
                          if failure["database"] not in selected]
    merged["samples"] = len(retained) + len(rows)
    merged["updated_utc"] = current["finished_utc"]
    merged.setdefault("updates", []).append({"databases": selected, "run": relative_run,
                                             "finished_utc": current["finished_utc"]})
    staging = run_directory / "merged"
    staging.mkdir()
    (staging / "metadata.json").write_text(json.dumps(merged, indent=2, ensure_ascii=False) + "\n")
    (staging / "raw.jsonl").write_text(combined)
    write_csv(staging / "raw.csv", [json.loads(line) for line in combined.splitlines()])
    subprocess.run([sys.executable, str(HERE / "plot.py"), str(staging)], check=True)
    # Preserve the old report and replaced logs before changing any report file.
    backup = run_directory / "previous"
    backup.mkdir()
    paths = [Path(name) for name in REPORT_FILES]
    (staging / "logs").mkdir()
    for source in (run_directory / "logs").iterdir():
        if source.is_file() and any(source.name.startswith(f"{database}-") for database in selected):
            paths.append(Path("logs") / source.name)
            shutil.copy2(source, staging / "logs" / source.name)
    for path in paths:
        if (directory / path).exists():
            (backup / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(directory / path, backup / path)
    if snapshot(directory) != original:
        raise RuntimeError("existing results changed while rendering; update was not applied")
    replaced = []
    try:
        for path in paths:
            (directory / path).parent.mkdir(parents=True, exist_ok=True)
            (staging / path).replace(directory / path)
            replaced.append(path)
    except BaseException:
        for path in reversed(replaced):
            if (backup / path).exists():
                shutil.copy2(backup / path, directory / path)
            else:
                (directory / path).unlink()
        raise
    shutil.rmtree(staging)
    print(f"Updated only {', '.join(selected)}: {directory / 'report.html'}", flush=True)
    print(f"Previous report and replaced samples: {backup}", flush=True)


def docker(*args, check=True, timeout=120):
    result = subprocess.run(["docker", *map(str, args)], capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"docker {' '.join(map(str, args))}:\n{result.stderr.strip()}\n{result.stdout.strip()}")
    return result


def inspect_image(image):
    result = docker("image", "inspect", image)
    image = json.loads(result.stdout)[0]
    return {key: image.get(key) for key in ["Id", "RepoDigests", "Architecture", "Os", "Created"]}


def configure_database(definition, threads, memory_mib):
    definition = copy.deepcopy(definition)
    args = definition["args"]
    for flag in ["--io-threads", "--server-threads", "-t"]:
        if flag in args:
            args[args.index(flag) + 1] = str(threads)
    for index, value in enumerate(args):
        if value.startswith("--proactor_threads="):
            args[index] = f"--proactor_threads={threads}"
        elif value.startswith("--maxmemory="):
            args[index] = f"--maxmemory={memory_mib * 2**20}"
    for flag, value in [("--maxmemory", memory_mib * 2**20), ("-m", memory_mib)]:
        if flag in args:
            args[args.index(flag) + 1] = str(value)
    return definition


def cpu_set(value):
    cpus = []
    for part in value.split(","):
        bounds = [int(n) for n in part.split("-")]
        if len(bounds) == 1:
            cpus.append(bounds[0])
        elif len(bounds) == 2 and bounds[0] <= bounds[1]:
            cpus.extend(range(bounds[0], bounds[1] + 1))
        else:
            raise ValueError("invalid CPU range")
    if not cpus or min(cpus) < 0 or len(set(cpus)) != len(cpus):
        raise ValueError("invalid or duplicate CPU IDs")
    return set(cpus)


def memory_socket():
    # Match Docker CLI precedence: explicit context overrides DOCKER_HOST.
    if os.environ.get("DOCKER_HOST") and not os.environ.get("DOCKER_CONTEXT"):
        endpoint = os.environ["DOCKER_HOST"]
    else:
        endpoint = json.loads(docker("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}").stdout)
    parsed = urlparse(endpoint)
    if parsed.scheme != "unix":
        raise ValueError("RAM sampling requires a local Docker Engine/Desktop context with a unix:// socket")
    return unquote(parsed.path)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--databases")
    parser.add_argument("--clients")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--warmup-seconds", type=float)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--keys", type=int)
    parser.add_argument("--value-bytes", type=int)
    parser.add_argument("--server-cpuset")
    parser.add_argument("--client-cpuset")
    parser.add_argument("--server-threads", type=int)
    parser.add_argument("--client-threads", type=int)
    parser.add_argument("--server-memory")
    parser.add_argument("--database-memory-mib", type=int)
    parser.add_argument("--network-mode", choices=["bridge", "loopback"])
    destination = parser.add_mutually_exclusive_group()
    destination.add_argument("--output", type=Path)
    destination.add_argument("--update", type=Path, help="replace only selected databases in an existing report; inherit its measurement settings")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--pull", action="store_true")
    parser.add_argument("--quick", action="store_true", help="functional smoke run: 1/8 clients, 0.2s, no warmup, one repeat")
    parser.add_argument("--no-plots", action="store_true")
    args = parser.parse_args(argv)
    if args.clients is not None:
        try:
            args.clients = [int(n) for n in args.clients.split(",")]
        except ValueError:
            parser.error("--clients must be a comma-separated list of integers")
    defaults = PARAMETERS
    if args.update:
        if not args.databases:
            parser.error("--update requires --databases (for example, avalanche)")
        if args.quick or args.no_plots:
            parser.error("--update inherits the original settings and rebuilds plots; omit --quick and --no-plots")
        args.update = args.update.resolve()
        try:
            original = json.loads(snapshot(args.update)["metadata.json"])
        except (OSError, ValueError) as error:
            parser.error(f"cannot read existing results: {error}")
        defaults = {**PARAMETERS, **original["settings"]}
        for key in PARAMETERS:
            if getattr(args, key) is not None and getattr(args, key) != defaults[key]:
                parser.error(f"--update requires the original {key}={defaults[key]}")
        if any(name not in defaults["databases"] for name in args.databases.split(",")):
            parser.error("--update can only replace databases selected in the original run")
    for key in PARAMETERS:
        if getattr(args, key) is None:
            setattr(args, key, defaults[key])
    if args.quick:
        args.clients, args.seconds, args.warmup_seconds, args.repeats = [1, 8], 0.2, 0, 1
    args.databases = (args.databases or "avalanche,redis,valkey,memcached,dragonfly,keydb").split(",")
    if len(set(args.databases)) != len(args.databases) or len(set(args.clients)) != len(args.clients):
        parser.error("duplicate databases or client counts")
    if min(args.clients) <= 0 or args.seconds <= 0 or args.warmup_seconds < 0 or args.repeats <= 0 or args.keys <= 0 or args.value_bytes < 0:
        parser.error("invalid nonpositive benchmark parameter")
    try:
        server_cpus, client_cpus = cpu_set(args.server_cpuset), cpu_set(args.client_cpuset)
    except ValueError as error:
        parser.error(str(error))
    if server_cpus & client_cpus:
        parser.error("server and client CPU sets must be disjoint")
    if not 0 < args.server_threads <= len(server_cpus) or not 0 < args.client_threads <= len(client_cpus):
        parser.error("thread counts must be positive and fit their CPU sets")
    if args.database_memory_mib <= 0:
        parser.error("--database-memory-mib must be positive")
    return args


def main():
    args = arguments()
    original = snapshot(args.update) if args.update else None
    definitions = json.loads((HERE / "databases.json").read_text())
    if any(name not in definitions for name in args.databases):
        raise ValueError("unknown database")
    info = json.loads(docker("info", "--format", "{{json .}}").stdout)
    socket_path = memory_socket()
    arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}.get(info["Architecture"], info["Architecture"])
    target_platform = f"linux/{arch}"
    if max(cpu_set(args.server_cpuset) | cpu_set(args.client_cpuset)) >= info["NCPU"]:
        raise RuntimeError("selected CPU sets exceed the Docker host's available CPUs")
    if args.build:
        build_network = ["--network", "host"] if args.network_mode == "loopback" else []
        subprocess.run(["docker", "build", *build_network, "--platform", target_platform, "-f", str(HERE / "Dockerfile"), "-t", CLIENT_IMAGE, str(ROOT)], check=True)
    if args.pull:
        for name in args.databases:
            if name != "avalanche":
                subprocess.run(["docker", "pull", "--platform", target_platform, definitions[name]["image"]], check=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.update / "updates" / f"{stamp}-{uuid.uuid4().hex[:8]}" if args.update
              else args.output or HERE / "results" / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    metadata = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count()},
        "docker": {key: info[key] for key in ["ServerVersion", "Architecture", "NCPU", "MemTotal", "KernelVersion", "OperatingSystem"]},
        "settings": {key: value for key, value in vars(args).items() if key not in ["output", "update"]},
        "method": {"pipeline": 1, "key_bytes": 8, "get_hit_rate": 1.0,
                   "avalanche_command_execution": "inline in the connection task; command_worker_count is unused",
                   "server_memory_limit": args.server_memory, "client_memory_limit": "1g",
                   "latency_histogram": "microseconds; upper bounds of 32 sub-buckets per power of two",
                   "rps": "fully validated responses completed inside the fixed measurement window / seconds",
                   "network": ("shared isolated network namespace, loopback; no published ports" if args.network_mode == "loopback"
                               else "internal Docker bridge; no published ports; native architecture, no emulation"),
                   "logging": "application request logging disabled; server startup/error logs retained",
                   "cpu_policy": f"disjoint cpusets; {args.client_threads} client threads; {args.server_threads} server worker/I/O threads (database-dependent)"},
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in sorted((ROOT / "src").rglob("*.rs"))},
        "client_image": inspect_image(CLIENT_IMAGE), "databases": {},
    }
    metadata_path = output / "metadata.json"

    def save_metadata():
        metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n")

    save_metadata()
    suffix = uuid.uuid4().hex[:10]
    network = f"avalanche-comparison-{suffix}"
    server_name = f"{network}-server"
    client_name = f"{network}-client"
    if args.network_mode == "bridge":
        docker("network", "create", "--internal", network)
    server_network = (["--network", network, "--network-alias", "database"] if args.network_mode == "bridge"
                      else ["--network", "none"])
    client_network = network if args.network_mode == "bridge" else f"container:{server_name}"
    database_host = "database" if args.network_mode == "bridge" else "127.0.0.1"
    rows = []
    failures = []
    # Stable randomized order avoids always benchmarking one backend on a cold CPU.
    order = list(args.databases)
    random.Random(42).shuffle(order)
    metadata["database_order"] = order
    save_metadata()
    print(f"Results: {output}", flush=True)
    print(f"Native {target_platform}; servers CPU {args.server_cpuset}, client CPU {args.client_cpuset}", flush=True)
    try:
        with (output / "raw.jsonl").open("w") as raw:
            for database in order:
                definition = configure_database(definitions[database], args.server_threads, args.database_memory_mib)
                image_info = inspect_image(definition["image"])
                if image_info["Architecture"] != arch:
                    raise RuntimeError(f"{database}: image architecture {image_info['Architecture']} differs from {arch}")
                metadata["databases"][database] = {"definition": definition, "image": image_info,
                                                   "memory_measurement": MEMORY_METHOD}
                save_metadata()
                print(f"\nStarting {database}", flush=True)
                try:
                    # Dragonfly uses io_uring, which Docker's default seccomp
                    # profile blocks. Use the same setting for all servers and
                    # clients; containers have no host mounts or published ports.
                    docker("run", "-d", "--name", server_name, *server_network, "--platform", target_platform,
                           "--cpuset-cpus", args.server_cpuset, "--cpus", args.server_threads, "--memory", args.server_memory,
                           "--security-opt", "seccomp=unconfined", "--ulimit", "memlock=-1:-1",
                           "--env", f"BENCH_SERVER_THREADS={args.server_threads}",
                           "--entrypoint", definition["entrypoint"], definition["image"], *definition["args"])
                    # Check process state before starting correctness and load phases.
                    time.sleep(0.5)
                    state = json.loads(docker("inspect", server_name).stdout)[0]
                    metadata["databases"][database]["container_limits"] = {
                        key: state["HostConfig"][key] for key in ["CpusetCpus", "NanoCpus", "Memory", "SecurityOpt"]}
                    if not state["State"]["Running"]:
                        raise RuntimeError(docker("logs", server_name, check=False).stdout + docker("logs", server_name, check=False).stderr)
                    if definition["version"]:
                        version = docker("exec", server_name, *definition["version"], check=False)
                        metadata["databases"][database]["version"] = (version.stdout + version.stderr).strip().splitlines()[0]
                    else:
                        metadata["databases"][database]["version"] = "Avalanche 0.1.0 / benchmark fixture / source hashes recorded"
                    save_metadata()
                    # Execute the three actual Rust test functions for this DB
                    # before the longer, configurable performance sweep.
                    tests = docker("run", "--rm", "--name", client_name, "--network", client_network,
                        "--platform", target_platform, "--cpuset-cpus", args.client_cpuset,
                        "--cpus", args.client_threads, "--memory", "1g", "--security-opt", "seccomp=unconfined",
                        "--env", f"BENCH_ADDR={database_host}:{definition['port']}",
                        "--env", f"BENCH_TEST_THREADS={args.client_threads}",
                        "--env", "BENCH_TEST_CLIENTS=2", "--env", "BENCH_TEST_SECONDS=0.1",
                        "--env", "BENCH_TEST_WARMUP_SECONDS=0", "--env", "BENCH_TEST_REPEATS=1",
                        "--entrypoint", f"/opt/benchmark-tests/{database}", CLIENT_IMAGE,
                        "--ignored", "--nocapture", "--test-threads=1")
                    (output / "logs" / f"{database}-rust-tests.log").write_text(tests.stdout + tests.stderr)
                    metadata["databases"][database]["rust_tests"] = "3 passed"
                    save_metadata()
                    print("  GET / SET / mixed Rust tests: 3 passed", flush=True)
                    for mode in ["get", "set", "mixed"]:
                        for clients in args.clients:
                            print(f"  {database:10s} {mode:5s} clients={clients:3d} ({args.repeats} repeats)", flush=True)
                            with MemorySampler(socket_path, server_name,
                                               output / "logs" / f"{database}-{mode}-{clients}-memory.jsonl") as memory:
                                result = docker("run", "--rm", "--name", client_name, "--network", client_network,
                                    "--platform", target_platform, "--cpuset-cpus", args.client_cpuset,
                                    "--cpus", args.client_threads, "--memory", "1g", "--security-opt", "seccomp=unconfined",
                                    CLIENT_IMAGE, "--database", database, "--address", f"{database_host}:{definition['port']}",
                                    "--mode", mode, "--clients", clients, "--seconds", args.seconds,
                                    "--warmup-seconds", args.warmup_seconds, "--repeats", args.repeats,
                                    "--keys", args.keys, "--value-bytes", args.value_bytes, "--threads", args.client_threads,
                                    timeout=max(60, args.repeats * (args.seconds + args.warmup_seconds + 5) + 30))
                            (output / "logs" / f"{database}-{mode}-{clients}.log").write_text(result.stdout + result.stderr)
                            samples = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
                            if len(samples) != args.repeats:
                                raise RuntimeError(f"{database}/{mode}/{clients}: expected {args.repeats} samples, got {len(samples)}")
                            for sample in samples:
                                memory.attach(sample)
                                rows.append(sample)
                                raw.write(json.dumps(sample) + "\n")
                            raw.flush()
                            rates = sorted(sample["rps"] for sample in samples)
                            print(f"    RPS {rates[len(rates)//2]:,.0f}; errors {sum(sample['errors'] for sample in samples)}", flush=True)
                            print(f"    RAM peak {max(sample['ram_peak_bytes'] for sample in samples) / 2**20:.2f} MiB", flush=True)
                except Exception as error:
                    failures.append({"database": database, "error": str(error)})
                    print(f"FAILED {database}: {error}", file=sys.stderr, flush=True)
                finally:
                    logs = docker("logs", server_name, check=False)
                    (output / "logs" / f"{database}-server.log").write_text(logs.stdout + logs.stderr)
                    docker("rm", "-f", server_name, check=False)
                    docker("rm", "-f", client_name, check=False)
    finally:
        docker("rm", "-f", client_name, check=False)
        docker("rm", "-f", server_name, check=False)
        if args.network_mode == "bridge":
            docker("network", "rm", network, check=False)
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        metadata["failures"] = failures
        metadata["samples"] = len(rows)
        save_metadata()
        if rows:
            write_csv(output / "raw.csv", rows)
    if args.update and not failures:
        merge_update(args.update, output, original)
    elif not args.no_plots and rows and not args.update:
        subprocess.run([sys.executable, str(HERE / "plot.py"), str(output)], check=True)
    print(f"\nSaved {len(rows)} samples. Failures: {len(failures)}. {output}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
