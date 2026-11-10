# Benchmark setup and reproduction

[Back to Avalanche](../../README.md)

Run commands from the repository root. The README uses the full `trade2` run,
measured on **November 10, 2026, 12:24–12:35 MSK (UTC+03:00)**.

## Measurement setup

| Setting | Value |
| --- | --- |
| Host | `trade2`, AMD Ryzen 7 9700X, 8 physical cores / 16 SMT threads, 59.44 GiB RAM |
| OS / containers | Ubuntu 26.04.1 LTS; Linux/amd64; Docker Engine 29.2.1 |
| Server | 14 worker/I/O threads, CPU set `0-6,8-14`, quota 14 logical CPUs, 8 GiB memory limit |
| Client | 2 Tokio threads, CPU set `7,15`, quota 2 logical CPUs, 1 GiB memory limit |
| Topology | Server uses both SMT siblings of physical cores 0–6; client uses both siblings of core 7 |
| Network | Loopback in a shared isolated network namespace, separate containers/cgroups, no published ports |
| Connections | 1, 8, 32, 128, 512; one outstanding request per connection |
| Dataset | 1,024 eight-byte hexadecimal keys, 256-byte binary values |
| Workloads | GET hits; SET overwrites; alternating 50/50 GET/SET with independent pseudorandom key selection |
| Timing | 0.5-second warmup before each 2-second window; three repeats per point |
| Data memory configuration | 4 GiB for Redis/Valkey/Dragonfly/KeyDB/Memcached; Avalanche limited by its 8 GiB container |
| Features | Persistence, replication, TTL, TLS and client pipelining were not exercised |

Avalanche was built from the root Cargo package's `bench-server` binary,
using the shared application library and its release profile (`opt-level=3`,
thin LTO, one codegen unit). The client and Rust database tests use the separate
comparison Cargo package. All image digests, launch arguments and source hashes
are saved. Fourteen threads means Tokio workers for Avalanche, Memcached workers,
Dragonfly proactors, KeyDB server threads, and Redis/Valkey I/O threads; command
execution in Redis/Valkey is not thereby parallelized across fourteen workers.

The current source has 256 Papaya maps. The shard index uses the first two bytes;
all measured keys start with `00` and select index 96. The run therefore exercises
one map for the measured dataset. Each database starts fresh, then remains running
across workloads and connection counts. All CPUs are logical CPU IDs, not fourteen
physical cores. SMT siblings are assigned together so the main client and server
do not share a physical core.

RPS counts complete, byte-validated responses received inside each fixed window.
Population, connections, warmup and responses drained after the deadline are excluded.
Latency includes sending and validating the response. Histograms report upper
bounds of microsecond buckets with 32 subdivisions per power of two; the README
uses medians of per-repeat p99 values.

RAM is server-container working set: cgroup usage minus inactive file cache.
The Docker API is sampled every 100 ms outside the server container. Only samples
inside each repeat's load window are included. Average RAM is the median of repeat
averages; peak RAM is the maximum sampled peak. Client RAM is excluded, short spikes
may be missed, and allocator-retained memory from earlier phases remains included.

The client reached a median of 1.97 logical CPUs in sampled high-concurrency
measurement intervals. A separate client sensitivity check uses a wider CPU set
and fourteen client threads, while retaining fourteen server workers. Its CPU set
overlaps the server's and its numbers are reported separately from the main comparison.
KeyDB's high-concurrency variability was checked with a fresh server process and
three longer five-second mixed measurement windows. These results characterize
this host, key distribution, load generator and network path; they do not isolate
the effect of changing only the server thread count from the earlier ARM64 run.

## Recorded data

The data and three charts used by the README are in
[`assets/trade2-14t-20261110/`](assets/trade2-14t-20261110/). The full run contains **270 measurements**,
**302,686,199 validated responses**, **zero request errors**, and **18 passed database tests**.
The client control has twelve
measurements; the longer KeyDB confirmation has three.

- [Raw JSONL](assets/trade2-14t-20261110/raw.jsonl),
  [summary](assets/trade2-14t-20261110/summary.csv), and [metadata](assets/trade2-14t-20261110/metadata.json).
- [Recorded verification summary](assets/trade2-14t-20261110/verification.json)
  and [hardware/topology](assets/trade2-14t-20261110/hardware.json).
- Client control [summary](assets/trade2-14t-20261110/client-control-summary.csv),
  [raw measurements](assets/trade2-14t-20261110/client-control-raw.jsonl), and
  [configuration](assets/trade2-14t-20261110/client-control-metadata.json).
- [Throughput](assets/trade2-14t-20261110/throughput.png),
  [p99 latency](assets/trade2-14t-20261110/latency_p99.png), and
  [RAM](assets/trade2-14t-20261110/memory.png) charts.
- Longer-window
  [KeyDB confirmation](assets/trade2-14t-20261110/keydb-confirmation.json).

Generated reports, historical runs, logs, binaries, source snapshots, and evidence
archives are not retained. The verification summary records checks performed during
the original run; repeating those checks requires a new run. The retained raw
measurements and metadata are sufficient to regenerate the README charts.

## Local Avalanche comparison without Docker

The native runner builds Avalanche in release mode and starts the server and load
generator as separate processes on `127.0.0.1`, using an automatically selected
port. It supports macOS and Linux and requires Rust, Cargo, Git, and Python 3.12+.
Docker and Python packages are not required. If the comparison `.venv` already
contains matplotlib, the runner also generates PNG/SVG charts; otherwise CSV,
the console table, and the HTML report are still produced.

Save the current working-tree version as the first baseline:

```sh
bash tests/comparison/run-local.sh --no-compare
```

After changing the application, run:

```sh
bash tests/comparison/run-local.sh
```

The second command automatically finds the latest successful native run in
`tests/comparison/results/local/`, copies its saved server binary, and **remeasures
both the previous binary and the current version** with the current load generator.
It does not reuse old performance measurements. With no previous native run, it
measures the current version and saves it for next time.

The load generator detects Avalanche's response format during the untimed
missing-key check. It supports both legacy one-byte statuses and the current
four-byte status with a separate payload length. Each variant's detected format
is recorded as `response_protocol` in `metadata.json`; protocol differences are
included in the comparison. The load generator always comes from the working-tree
snapshot, including when `--current-ref` selects a server from Git.

To compare uncommitted changes against the committed version, without needing an
earlier local run:

```sh
bash tests/comparison/run-local.sh --baseline-ref HEAD
```

To compare two Git revisions, or explicitly choose a saved native run:

```sh
bash tests/comparison/run-local.sh --current-ref HEAD --baseline-ref HEAD~1
bash tests/comparison/run-local.sh --baseline tests/comparison/results/local/RUN_DIRECTORY
```

Git revisions are exported into isolated source directories and built without
checking out files or changing the working tree. Each revision must contain the
comparison Cargo package and its server fixture, and must use a protocol supported
by the current load generator. Saved binaries must match the native host target;
their SHA256 hashes are checked before reuse. Keep the complete saved run directory
to retain its binary and provenance. Custom `--output` directories can be selected
as baselines explicitly with `--baseline`.

Before each build, the runner clears only the compiled `avalanche-comparison`
package, keeping its dependency cache. This forces the application modules outside
that package to be recompiled from the selected snapshot instead of reusing a
binary built from an earlier snapshot.

The defaults are GET, SET, and a 50/50 mix, at 1/8/32/128 connections, 256-byte
values, 1,024 keys, one server worker thread per available logical CPU, and
4 client threads. `--server-threads N` overrides the automatic server count.
Each point has three 2-second measurements with 0.5 seconds of warmup before each
measurement.
`--quick` selects a short functional check; explicit workload flags override it:

```sh
bash tests/comparison/run-local.sh --baseline-ref HEAD --quick
bash tests/comparison/run-local.sh --clients 8,32,128 --seconds 10 \
  --warmup-seconds 2 --repeats 5 --server-threads 4 --client-threads 4
bash tests/comparison/run-local.sh --modes get --clients 32,128 --no-plots
```

Both versions receive the same workload and thread settings. Each point uses a
fresh server; the baseline/current order alternates between points. The generator
checks missing keys, binary SET/GET, and overwrites before populating the key space.
It validates every measured response and excludes late responses from RPS.
The script stops its server processes on completion, failure, or Ctrl+C. Concurrent
instances of the local runner are rejected to avoid overlapping measurements.

The current `bench-server` and `avalanche` binaries share `src/lib.rs`, dependencies,
and the root `Cargo.toml` release profile. The load generator remains a separate
package. Source snapshots include the root manifest and lockfile; historical Git
revisions with the standalone server fixture remain supported.

When starting `bench-server` directly, it uses
`std::thread::available_parallelism()` unless `BENCH_SERVER_THREADS` is set.
The selected count is logged as `AVALANCHE_SERVER_THREADS=N`. Inside Docker,
automatic detection respects the CPUs available to the container. The Docker
comparison command below explicitly selects fourteen server threads and fourteen
server CPUs, matching the published measurements.

Every run creates a new directory under `tests/comparison/results/local/` with:

- `metadata.json`: workload, native target, compiler, source hashes, binary hashes,
  selected baseline, and completion status.
- `sources/` and `bin/`: source snapshots and the executable binaries.
- `current/` and, when comparing, `baseline/`: raw JSONL/CSV and per-point summaries.
- `comparison.csv`: baseline/current median RPS, repeat ranges, p50/p95/p99, and
  percentage changes. Positive RPS change means higher throughput; negative p99
  change means lower latency.
- `report.html`, optional `comparison.png` / `.svg`, and build/client/server logs.

The local processes share the host scheduler, without CPU pinning or container
quotas. These results are separate from the Docker comparison above. Docker data
and failed or interrupted native runs cannot be selected as saved baselines.
Short smoke checks establish functionality; use longer windows and repeat ranges
to assess performance differences.

## Reproduce the Docker comparison

Requirements: Docker Desktop or Docker Engine with 16 available logical CPUs and
at least 12 GiB of memory for the 14-thread profile, Python 3.12 or newer with `venv` support, and internet access
for the initial image and dependency downloads. Rust compilation happens inside
the benchmark image.

From the repository root, run all six databases with the settings used above:

```sh
bash tests/comparison/run.sh --build --pull \
  --server-threads 14 --client-threads 2 \
  --server-cpuset 0-6,8-14 --client-cpuset 7,15 \
  --server-memory 8g --database-memory-mib 4096 \
  --network-mode loopback --clients 1,8,32,128,512
```

The runner builds Avalanche from the current source, runs correctness tests,
measures the workloads, generates charts, and removes its containers and network.
It creates a new directory under `tests/comparison/results/` and leaves previous
runs intact. The recorded five-level sweep takes about 12 minutes after setup.

For a quick functional check:

```sh
bash tests/comparison/run.sh --build --pull --quick
```

To refresh only Avalanche while retaining the other databases' existing data:

```sh
bash tests/comparison/run.sh --databases avalanche --build \
  --update tests/comparison/results/RUN_DIRECTORY
```

The runner inherits the original measurement settings, archives replaced results,
and rebuilds the combined report. Other databases are not started.

For a new report measuring only Avalanche:

```sh
bash tests/comparison/run.sh --databases avalanche --build
```

For longer measurements:

```sh
bash tests/comparison/run.sh --build --pull \
  --clients 1,8,32,128 \
  --seconds 10 --warmup-seconds 2 --repeats 5 \
  --keys 100000 --value-bytes 1024
```

To regenerate the English README charts after the benchmark environment has been
installed:

```sh
tests/comparison/.venv/bin/python docs/benchmarks/render.py \
  docs/benchmarks/assets/trade2-14t-20261110 \
  docs/benchmarks/assets/trade2-14t-20261110
```

The detailed [comparison guide](../../tests/comparison/README.md) (in Russian) covers
individual database tests, configuration, and updating selected databases in an
existing report. The pinned images and server arguments are in
[`databases.json`](../../tests/comparison/databases.json).

## Build and test

For local development, use a Rust toolchain supporting edition 2024 and the
dependencies in `Cargo.toml`:

```sh
cargo build --release
cargo test
```

To start the server using the benchmark configuration on a local address:

```sh
BENCH_SERVER_ADDR=127.0.0.1:8080 \
cargo run --release --bin bench-server
```

To run the standalone Avalanche load tests:

```sh
cargo test --release --test load -- --ignored --nocapture --test-threads=1
```

These standalone tests share a process and runtime with the server; their results
are separate from the isolated Docker comparison above. See the
[test guide](../../tests/README.md) (in Russian) for workload environment variables.
