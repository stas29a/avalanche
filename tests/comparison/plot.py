#!/usr/bin/env python3
"""Render comparison charts from measured samples, never from claimed vendor RPS."""
from collections import defaultdict
import csv
import html
import json
import os
from pathlib import Path
import re
import statistics
import sys
import tempfile

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "avalanche-comparison-matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "avalanche-comparison-cache"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator
from memory import summarize_memory

ORDER = ["avalanche", "redis", "valkey", "memcached", "dragonfly", "keydb"]
NAMES = {"avalanche": "Avalanche", "redis": "Redis", "valkey": "Valkey", "memcached": "Memcached", "dragonfly": "Dragonfly", "keydb": "KeyDB"}
COLORS = dict(zip(ORDER, ["#2563eb", "#dc2626", "#0d9488", "#7c3aed", "#ca8a04", "#ea580c"]))
MARKERS = dict(zip(ORDER, ["o", "s", "D", "^", "v", "P"]))
MODES = ["get", "set", "mixed"]
TITLES = {"get": "GET", "set": "SET", "mixed": "50% GET + 50% SET"}


def main(directory):
    directory = Path(directory).resolve()
    metadata = json.loads((directory / "metadata.json").read_text())
    rows = [json.loads(line) for line in (directory / "raw.jsonl").read_text().splitlines() if line]
    if not rows:
        raise ValueError("no measured samples to plot")
    groups = defaultdict(list)
    for row in rows:
        groups[row["database"], row["mode"], row["clients"]].append(row)
    summary = []
    for (database, mode, clients), samples in sorted(groups.items()):
        summary.append({
            "database": database, "mode": mode, "clients": clients, "repeats": len(samples),
            "rps_median": statistics.median(row["rps"] for row in samples),
            "rps_min": min(row["rps"] for row in samples),
            "rps_max": max(row["rps"] for row in samples),
            "p50_us_median": statistics.median(row["p50_us"] for row in samples),
            "p95_us_median": statistics.median(row["p95_us"] for row in samples),
            "p99_us_median": statistics.median(row["p99_us"] for row in samples),
            "errors": sum(row["errors"] for row in samples),
            **summarize_memory(samples),
        })
    with (directory / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader(); writer.writerows(summary)

    settings = metadata["settings"]
    cpus = settings["server_cpuset"]
    note = (f"Linux/{metadata['client_image']['Architecture']} · server CPU {cpus}, client CPU {settings['client_cpuset']} · "
            f"{settings['value_bytes']} B · {settings['keys']} keys · pipeline=1\n"
            f"Threads {settings.get('server_threads', 4)} server / {settings.get('client_threads', 4)} client. "
            f"Median of {settings['repeats']} runs × {settings['seconds']:g} s; warmup {settings['warmup_seconds']:g} s. "
            "Bands / whiskers: min–max. Fully validated responses; errors excluded.")
    if settings.get("network_mode") == "loopback":
        note += "\nNetwork: shared isolated network namespace / loopback; separate server and client cgroups."
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.spines.top": False,
                         "axes.spines.right": False, "axes.titleweight": "bold", "svg.fonttype": "none"})

    def save(fig, name):
        fig.savefig(directory / f"{name}.png", dpi=180, facecolor="white", bbox_inches="tight")
        fig.savefig(directory / f"{name}.svg", facecolor="white", bbox_inches="tight")
        plt.close(fig)

    for metric, filename, title in [
        ("rps_median", "throughput", "Пропускная способность по числу TCP-клиентов — выше лучше"),
        ("p99_us_median", "latency_p99", "Задержка p99 по числу TCP-клиентов — ниже лучше"),
    ]:
        fig, axes = plt.subplots(1, 3, figsize=(17, 5.8), sharex=True, sharey=True)
        for axis, mode in zip(axes, MODES):
            for database in ORDER:
                points = sorted((row for row in summary if row["database"] == database and row["mode"] == mode), key=lambda row: row["clients"])
                if not points:
                    continue
                x = [point["clients"] for point in points]
                factor = 1000
                y = [point[metric] / factor for point in points]
                axis.plot(x, y, color=COLORS[database], marker=MARKERS[database], linewidth=2.2, markersize=5, label=NAMES[database])
                if metric == "rps_median":
                    axis.fill_between(x, [p["rps_min"] / factor for p in points], [p["rps_max"] / factor for p in points], color=COLORS[database], alpha=0.09)
            axis.set_title(TITLES[mode], pad=12)
            axis.set_xscale("log", base=2)
            axis.set_xticks(settings["clients"], labels=list(map(str, settings["clients"])))
            axis.set_xlabel("TCP-клиенты (логарифмическая шкала)")
            axis.set_ylabel("тыс. успешных запросов/с" if metric == "rps_median" else "p99, мс (логарифмическая шкала)")
            if metric == "rps_median":
                axis.set_ylim(0, max(row["rps_max"] for row in summary) / 1000 * 1.12)
            else:
                axis.set_yscale("log")
                axis.yaxis.set_major_locator(LogLocator(base=10, subs=[1, 2, 5]))
                axis.yaxis.set_minor_locator(NullLocator())
                axis.yaxis.set_major_formatter(FuncFormatter(lambda n, _: f"{n:g}"))
            axis.grid(True, alpha=0.2, which="major")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.suptitle(title, fontsize=18, fontweight="bold", y=1.03)
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.98), ncol=6, frameon=False)
        curve_note = note if metric == "rps_median" else note.replace("Bands / whiskers: min–max. ", "")
        fig.text(0.5, -0.035, curve_note, ha="center", va="top", fontsize=10, color="#475569")
        fig.tight_layout(rect=(0, 0, 1, 0.90))
        save(fig, filename)

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.8), sharex=True, sharey=True)
    for axis, mode in zip(axes, MODES):
        for database in ORDER:
            points = sorted((row for row in summary if row["database"] == database and row["mode"] == mode
                             and row["ram_repeats"]), key=lambda row: row["clients"])
            if not points:
                continue
            x = [point["clients"] for point in points]
            axis.plot(x, [p["ram_avg_bytes_median"] / 2**20 for p in points],
                      color=COLORS[database], marker=MARKERS[database], label=NAMES[database])
            axis.plot(x, [p["ram_peak_bytes_max"] / 2**20 for p in points],
                      color=COLORS[database], linestyle="--", marker="_", alpha=0.65)
        if not axis.lines:
            axis.text(0.5, 0.5, "Нет замеров RAM", transform=axis.transAxes, ha="center")
        axis.set_title(TITLES[mode], pad=12)
        axis.set_xscale("log", base=2)
        axis.set_xticks(settings["clients"], labels=list(map(str, settings["clients"])))
        axis.set_xlabel("TCP-клиенты (логарифмическая шкала)")
        axis.set_ylabel("RAM серверного контейнера, MiB")
        axis.set_ylim(bottom=0)
        axis.grid(True, alpha=0.2)
    legends = dict((label, handle) for axis in axes for handle, label in zip(*axis.get_legend_handles_labels()))
    fig.legend(legends.values(), legends.keys(), loc="upper center", bbox_to_anchor=(0.5, 0.98), ncol=6, frameon=False)
    fig.suptitle("Память серверов во время нагрузки", fontsize=18, fontweight="bold", y=1.03)
    fig.text(0.5, -0.035, f"{settings['keys']} keys × {settings['value_bytes']} B; {settings['repeats']} repeats × {settings['seconds']:g} s. "
             "Working set: usage − inactive_file; опрос каждые 100 мс.\n"
             "Сплошная линия: медиана среднего RAM; пунктир: максимум наблюдаемых пиков повторов. Старые замеры без RAM пропущены.",
             ha="center", va="top", fontsize=10, color="#475569")
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    save(fig, "memory")

    best = []
    for mode in MODES:
        for database in ORDER:
            candidates = [row for row in summary if row["database"] == database and row["mode"] == mode]
            if candidates:
                best.append(max(candidates, key=lambda row: row["rps_median"]))
    global_max = max(row["rps_max"] for row in best) / 1000
    fig, axes = plt.subplots(1, 3, figsize=(17, 6), sharex=True, sharey=True)
    for axis, mode in zip(axes, MODES):
        points = [row for row in best if row["mode"] == mode]
        y = list(range(len(points)))
        rates = [point["rps_median"] / 1000 for point in points]
        axis.barh(y, rates, color=[COLORS[p["database"]] for p in points], alpha=0.9, height=0.65)
        axis.errorbar(rates, y, xerr=[[(p["rps_median"] - p["rps_min"]) / 1000 for p in points], [(p["rps_max"] - p["rps_median"]) / 1000 for p in points]], fmt="none", ecolor="#334155", capsize=3)
        for index, point in enumerate(points):
            axis.text(point["rps_max"] / 1000 + global_max * 0.02, index, f"{rates[index]:.1f}k  ·  {point['clients']} clients", va="center", fontsize=9)
        axis.set_yticks(y, labels=[NAMES[p["database"]] for p in points])
        axis.set_xlim(0, global_max * 1.45)
        axis.invert_yaxis() if mode == "get" else None
        axis.set_title(TITLES[mode], pad=12)
        axis.set_xlabel("тыс. успешных запросов/с")
        axis.grid(axis="x", alpha=0.15)
        axis.set_axisbelow(True)
    fig.suptitle("Лучший медианный RPS среди проверенных уровней нагрузки", fontsize=18, fontweight="bold")
    fig.text(0.5, -0.025, note, ha="center", va="top", fontsize=10, color="#475569")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save(fig, "best_rps")

    with (directory / "best.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(best[0]))
        writer.writeheader(); writer.writerows(best)

    def mib(value):
        return "нет данных" if value is None else f"{value / 2**20:.2f}"

    table = "<tr><th>База</th><th>Режим</th><th>Клиенты</th><th>RPS: медиана</th><th>min–max</th><th>p95, мс</th><th>p99, мс</th><th>RAM среднее¹, MiB</th><th>RAM пик², MiB</th></tr>"
    for row in best:
        table += (f"<tr><td>{NAMES[row['database']]}</td><td>{TITLES[row['mode']]}</td><td>{row['clients']}</td>"
                  f"<td>{row['rps_median']:,.0f}</td><td>{row['rps_min']:,.0f}–{row['rps_max']:,.0f}</td>"
                  f"<td>{row['p95_us_median']/1000:.3f}</td><td>{row['p99_us_median']/1000:.3f}</td>"
                  f"<td>{mib(row['ram_avg_bytes_median'])}</td><td>{mib(row['ram_peak_bytes_max'])}</td></tr>")
    memory_table = "<tr><th>База</th><th>Режим</th><th>Клиенты</th><th>Повторов с RAM</th><th>RAM среднее¹, MiB</th><th>RAM пик², MiB</th></tr>"
    for row in summary:
        memory_table += (f"<tr><td>{NAMES[row['database']]}</td><td>{TITLES[row['mode']]}</td><td>{row['clients']}</td>"
                         f"<td>{row['ram_repeats']}/{row['repeats']}</td><td>{mib(row['ram_avg_bytes_median'])}</td>"
                         f"<td>{mib(row['ram_peak_bytes_max'])}</td></tr>")
    def plain_version(value):
        value = re.sub(r"\x1b\[[0-9;]*m", "", value)
        return re.sub(r"\\x1b\[[0-9;]*m", "", value)
    versions = "".join(
        f"<li><strong>{NAMES[name]}</strong>: {html.escape(plain_version(data.get('version', 'not started')))}"
        f" — замеры {html.escape(data.get('measured_utc', metadata.get('finished_utc', metadata['started_utc'])))}</li>"
        for name, data in metadata["databases"].items())
    failures = html.escape(json.dumps(metadata.get("failures", []), ensure_ascii=False, indent=2))
    report = f'''<!doctype html><html lang="ru"><meta charset="utf-8"><title>Сравнение in-memory KV</title>
<style>body{{font:16px system-ui;max-width:1500px;margin:40px auto;padding:0 24px;color:#172033}}img{{width:100%}}table{{border-collapse:collapse;width:100%}}td,th{{padding:10px;border-bottom:1px solid #ddd;text-align:left}}pre{{white-space:pre-wrap}}a{{color:#2563eb}}</style>
<h1>Сравнение in-memory key-value серверов</h1><p>{html.escape(note)}</p>
<p>Каждая база работает отдельно, с теми же лимитами CPU/памяти. Клиент находится в отдельном контейнере.
Все GET обращаются к заполненным ключам; SET перезаписывает ограниченный набор ключей.
Значения проверяются побайтно. Сохранение на диск, репликация и подробные логи отключены.
p95/p99 — медианы перцентилей отдельных прогонов, с округлением вверх до границы корзины гистограммы.</p>
<img src="best_rps.png" alt="Лучший медианный RPS"><img src="throughput.png" alt="RPS по числу клиентов">
<img src="latency_p99.png" alt="p99 задержки"><h2>Лучшие замеры</h2><table>{table}</table>
<h2>Расход RAM каждой БД</h2><img src="memory.png" alt="RAM серверов по числу клиентов">
<p>RAM — working set всего серверного контейнера: usage минус inactive_file (total_inactive_file на cgroup v1),
по методике <a href="https://docs.docker.com/reference/cli/docker/container/stats/">Docker stats</a>.
Это не RSS одного процесса и не размер полезных данных. Память клиента исключена.
Сбор через Docker Engine API каждые 100 мс; фактическое число отсчётов есть в raw.csv (ram_samples).
Учитываются только отсчёты внутри окна нагрузки каждого повтора, без подготовки и прогрева.
¹ Медиана средних значений повторов. ² Максимум наблюдаемых пиков повторов; короткие всплески между отсчётами могут быть пропущены.
Старые результаты без RAM показаны как «нет данных». БД не перезапускается между режимами и уровнями нагрузки;
удержанная аллокатором память предыдущих этапов входит в замер.</p><table>{memory_table}</table>
<h2>Версии</h2><ul>{versions}</ul><h2>Ошибки запуска</h2><pre>{failures}</pre>
<p><a href="raw.csv">Сырые замеры CSV</a> · <a href="raw.jsonl">JSONL</a> · <a href="summary.csv">Все медианы</a> · <a href="metadata.json">Методика, образы и настройки</a></p>
<p>Это сравнение указанной нагрузки в Docker Desktop на данном компьютере. Различия протоколов входят в результат.
Проверенные уровни нагрузки ограничены; лучший RPS здесь не является гарантированным пределом базы.
Короткий набор из 1024 ключей помещается в кеши CPU и не характеризует базы с большим объёмом данных.</p></html>'''
    (directory / "report.html").write_text(report)
    print(f"Charts and report: {directory / 'report.html'}")


if __name__ == "__main__":
    main(sys.argv[1])
