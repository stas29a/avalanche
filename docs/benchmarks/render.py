#!/usr/bin/env python3
"""Render English README charts from a saved comparison run's raw measurements."""

import argparse
from collections import defaultdict
from datetime import datetime
import json
import os
from pathlib import Path
import statistics
import tempfile

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "avalanche-readme-matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "avalanche-readme-cache"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator


DATABASES = [
    ("avalanche", "Avalanche", "#2563eb", "o"),
    ("redis", "Redis", "#dc2626", "s"),
    ("valkey", "Valkey", "#0d9488", "D"),
    ("memcached", "Memcached", "#7c3aed", "^"),
    ("dragonfly", "Dragonfly", "#ca8a04", "v"),
    ("keydb", "KeyDB", "#ea580c", "P"),
]
MODES = [("get", "GET"), ("set", "SET"), ("mixed", "50% GET / 50% SET")]


def render(source, output):
    metadata = json.loads((source / "metadata.json").read_text())
    settings = metadata["settings"]
    rows = [json.loads(line) for line in (source / "raw.jsonl").read_text().splitlines() if line]
    if metadata.get("failures") or not rows or any(row["errors"] for row in rows):
        raise ValueError("README charts require a successful run without request errors")

    groups = defaultdict(list)
    for row in rows:
        groups[row["database"], row["mode"], row["clients"]].append(row)
    for database in settings["databases"]:
        for mode, _ in MODES:
            for clients in settings["clients"]:
                samples = groups[database, mode, clients]
                if len(samples) != settings["repeats"] or {
                    row["repeat"] for row in samples
                } != set(range(1, settings["repeats"] + 1)):
                    raise ValueError(f"incomplete measurements: {database}/{mode}/{clients}")

    output.mkdir(parents=True, exist_ok=True)
    date = datetime.fromisoformat(metadata["finished_utc"]).strftime("%Y-%m-%d")
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.spines.top": False, "axes.spines.right": False, "svg.fonttype": "none",
    })
    notes = (
        f"{date} | Linux/{metadata['client_image']['Architecture']} | "
        f"{settings['value_bytes']}-byte values | {settings['keys']:,} keys | "
        f"pipeline {metadata['method']['pipeline']}\n"
        f"Server CPUs {settings['server_cpuset']}; client CPUs {settings['client_cpuset']} | "
        f"Threads {settings.get('server_threads', 4)} server / {settings.get('client_threads', 4)} client | "
        f"Median of {settings['repeats']} x {settings['seconds']:g}s; "
        f"warmup {settings['warmup_seconds']:g}s | Shading: min-max of repeats"
    )
    if settings.get("network_mode") == "loopback":
        notes += "\nIsolated shared network namespace / loopback; separate server and client cgroups"
    if metadata.get("updates"):
        latest = metadata["updates"][-1]
        names = dict((key, name) for key, name, _, _ in DATABASES)
        refreshed = ", ".join(names[key] for key in latest["databases"])
        refreshed_at = datetime.fromisoformat(latest["finished_utc"]).strftime("%Y-%m-%d %H:%M UTC")
        baseline_at = datetime.fromisoformat(metadata["finished_utc"]).strftime("%Y-%m-%d %H:%M UTC")
        notes += f"\n{refreshed} refreshed {refreshed_at}; other databases retain the {baseline_at} measurements"
    metrics = [
        ("rps", "throughput", "Throughput by concurrent TCP connections", "Thousand successful requests / second", 1000),
        ("p99_us", "latency_p99", "Client-observed p99 latency by concurrent TCP connections", "p99 latency (microseconds)", 1),
    ]
    if all(row.get("ram_avg_bytes") is not None and row.get("ram_peak_bytes") is not None for row in rows):
        metrics.append(("ram_avg_bytes", "memory", "Server RAM during the measured workload", "Server container working set (MiB)", 2**20))
    for metric, filename, title, ylabel, divisor in metrics:
        memory = metric == "ram_avg_bytes"
        fig, axes = plt.subplots(1, 3, figsize=(16, 5.8), sharey=True)
        upper = max(row["ram_peak_bytes" if memory else metric] / divisor for row in rows) * 1.10
        for axis, (mode, label) in zip(axes, MODES):
            for database, name, color, marker in DATABASES:
                if database not in settings["databases"]:
                    continue
                clients = sorted(settings["clients"])
                points = [[row[metric] / divisor for row in groups[database, mode, n]] for n in clients]
                axis.plot(clients, [statistics.median(p) for p in points],
                          color=color, marker=marker, linewidth=2.2, markersize=5, label=name)
                if memory:
                    peaks = [max(row["ram_peak_bytes"] / divisor for row in groups[database, mode, n]) for n in clients]
                    axis.plot(clients, peaks, color=color, linestyle="--", marker="_", alpha=0.65)
                else:
                    axis.fill_between(clients, [min(p) for p in points], [max(p) for p in points],
                                      color=color, alpha=0.10)
            axis.set_title(label, fontweight="bold", pad=12)
            axis.set_xscale("log", base=2)
            axis.set_xticks(sorted(settings["clients"]), labels=sorted(settings["clients"]))
            if metric == "p99_us":
                axis.set_yscale("log")
                axis.set_ylim(min(row[metric] for row in rows) / 1.2, upper)
                axis.yaxis.set_major_locator(LogLocator(base=10, subs=[1, 2, 5]))
                axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
            else:
                axis.set_ylim(0, upper)
            axis.set_xlabel("Concurrent connections (log scale)")
            axis.grid(True, alpha=0.2)
        axes[0].set_ylabel(ylabel + (", log scale" if metric == "p99_us" else ""))
        handles, labels = axes[0].get_legend_handles_labels()
        fig.suptitle(title, fontsize=18, fontweight="bold", y=0.98)
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.91), ncol=6, frameon=False)
        chart_notes = notes
        if memory:
            chart_notes = notes.replace("Shading: min-max of repeats", "Solid: median of average RAM; dashed: maximum sampled peak")
            chart_notes += "\nContainer usage minus inactive file cache; sampled every 100ms inside the load window; short spikes may be missed"
        fig.text(0.5, 0.025, chart_notes, ha="center", va="bottom", fontsize=9, color="#475569")
        fig.tight_layout(rect=(0, 0.14, 1, 0.82))
        for extension in ["png", "svg"]:
            fig.savefig(output / f"{filename}.{extension}", dpi=160, facecolor="white")
        plt.close(fig)
    print(f"English charts saved to {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="saved benchmark run directory")
    parser.add_argument("output", type=Path, help="destination for English PNG/SVG charts")
    args = parser.parse_args()
    render(args.source.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
