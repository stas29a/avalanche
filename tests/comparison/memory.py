"""Sample server-container RAM without running a probe in the server's cgroup."""
from datetime import datetime
import http.client
import json
import socket
import statistics
import threading
import time
from urllib.parse import quote

INTERVAL_SECONDS = 0.1
METHOD = {
    "metric": "container_working_set",
    "source": "Docker Engine stats API, stream=false&one-shot=true",
    "formula": "usage - inactive_file (cgroup v2) / total_inactive_file (v1)",
    "interval_ms": INTERVAL_SECONDS * 1000,
    "window": "only samples inside each client's measurement window; Docker host clock",
    "average": "arithmetic mean of sampled bytes",
    "peak": "maximum sampled bytes, not an exact high-water mark",
    "scope": "server container only; includes allocator, connections and kernel memory; not process RSS",
}


def timestamp_ns(value):
    """Preserve the nanoseconds in Docker's RFC3339 timestamp."""
    whole, dot, fraction = value.rstrip("Z").partition(".")
    seconds = int(datetime.fromisoformat(whole + "+00:00").timestamp())
    return seconds * 1_000_000_000 + int((fraction if dot else "").ljust(9, "0")[:9])


def memory_sample(stats):
    memory = stats["memory_stats"]
    usage = memory["usage"]
    counters = memory["stats"]
    if "total_inactive_file" in counters:
        inactive = counters["total_inactive_file"]
    else:
        inactive = counters["inactive_file"]
    if not 0 <= inactive <= usage:
        raise ValueError(f"invalid container memory counters: usage={usage}, inactive_file={inactive}")
    return {"timestamp_unix_ns": timestamp_ns(stats["read"]),
            "usage_bytes": usage, "inactive_file_bytes": inactive,
            "working_set_bytes": usage - inactive}


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=5)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class MemorySampler:
    def __init__(self, socket_path, container, log_path):
        self.connection = UnixConnection(socket_path)
        self.url = f"/containers/{quote(container, safe='')}/stats?stream=false&one-shot=true"
        self.log_path = log_path
        self.samples = []
        self.error = None
        self.stop = threading.Event()
        self.thread = None

    def sample(self):
        self.connection.request("GET", self.url)
        response = self.connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"Docker RAM stats HTTP {response.status}: {body.decode(errors='replace')}")
        sample = memory_sample(json.loads(body))
        self.samples.append(sample)
        self.log.write(json.dumps(sample) + "\n")

    def collect(self):
        try:
            while not self.stop.is_set():
                began = time.monotonic()
                self.sample()
                self.stop.wait(max(0, INTERVAL_SECONDS - (time.monotonic() - began)))
        except Exception as error:
            self.error = error

    def __enter__(self):
        self.log = self.log_path.open("w")
        try:
            self.sample()  # Fail before starting load if stats are unavailable.
            self.thread = threading.Thread(target=self.collect, name="server-ram-sampler", daemon=True)
            self.thread.start()
        except BaseException:
            self.connection.close()
            self.log.close()
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop.set()
        self.thread.join()  # HTTP requests have a finite timeout.
        self.connection.close()
        self.log.close()
        if self.error is not None and exc_type is None:
            raise RuntimeError(f"RAM sampling failed: {self.error}") from self.error

    def attach(self, measurement):
        start = measurement.get("started_unix_ns")
        end = measurement.get("finished_unix_ns")
        if start is None or end is None:
            raise ValueError("client lacks RAM measurement timestamps; rebuild with --build")
        samples = [sample for sample in self.samples if start <= sample["timestamp_unix_ns"] < end]
        if not samples:
            raise ValueError("no RAM samples inside measurement window; increase --seconds")
        values = [sample["working_set_bytes"] for sample in samples]
        measurement.update(ram_metric=METHOD["metric"], ram_samples=len(samples),
                           ram_avg_bytes=statistics.mean(values), ram_peak_bytes=max(values),
                           ram_sample_interval_ms=METHOD["interval_ms"])


def summarize_memory(samples):
    measured = [row for row in samples if row.get("ram_avg_bytes") is not None
                and row.get("ram_peak_bytes") is not None]
    return {"ram_repeats": len(measured),
            "ram_avg_bytes_median": statistics.median(row["ram_avg_bytes"] for row in measured) if measured else None,
            "ram_peak_bytes_max": max(row["ram_peak_bytes"] for row in measured) if measured else None}
