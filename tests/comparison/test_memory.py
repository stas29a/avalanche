"""Verify comparable RAM accounting and strict measurement-window boundaries."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from memory import MemorySampler, memory_sample, summarize_memory, timestamp_ns


class MemoryTests(unittest.TestCase):
    def test_cgroup_v1_and_v2_subtract_only_inactive_file_cache(self):
        for counters in [{"inactive_file": 20, "file": 50, "anon": 40},
                         {"total_inactive_file": 20, "inactive_file": 3, "cache": 50}]:
            with self.subTest(counters=counters):
                sample = memory_sample({"read": "2026-11-10T12:00:00.123456789Z",
                                        "memory_stats": {"usage": 100, "stats": counters}})
                self.assertEqual(sample["working_set_bytes"], 80)
                self.assertEqual(sample["usage_bytes"], 100)
                self.assertEqual(sample["timestamp_unix_ns"] % 1_000_000_000, 123456789)
        self.assertEqual(timestamp_ns("1970-01-01T00:00:01Z"), 1_000_000_000)

    def test_missing_or_invalid_counters_are_not_reported_as_zero(self):
        for memory in [{}, {"usage": 100, "stats": {}},
                       {"usage": 100, "stats": {"inactive_file": 101}}]:
            with self.subTest(memory=memory), self.assertRaises((KeyError, ValueError)):
                memory_sample({"read": "2026-11-10T12:00:00Z", "memory_stats": memory})

    def test_each_repeat_excludes_seed_warmup_and_drained_responses(self):
        sampler = MemorySampler("unused", "server", Path("unused"))
        sampler.samples = [{"timestamp_unix_ns": stamp, "working_set_bytes": value}
                           for stamp, value in [(99, 900), (100, 10), (150, 30), (200, 800),
                                                (300, 40), (350, 60), (400, 700)]]
        for start, average, peak in [(100, 20, 30), (300, 50, 60)]:
            measurement = {"started_unix_ns": start, "finished_unix_ns": start + 100}
            sampler.attach(measurement)
            self.assertEqual(measurement["ram_avg_bytes"], average)
            self.assertEqual(measurement["ram_peak_bytes"], peak)
            self.assertEqual(measurement["ram_samples"], 2)
        with self.assertRaisesRegex(ValueError, "increase --seconds"):
            sampler.attach({"started_unix_ns": 500, "finished_unix_ns": 600})
        with self.assertRaisesRegex(ValueError, "rebuild"):
            sampler.attach({})

    def test_background_failure_stops_sampler_and_fails_the_run(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = MemorySampler("unused", "server", Path(directory) / "memory.jsonl")
            with patch.object(sampler, "sample", side_effect=[None, OSError("daemon stopped")]), \
                    self.assertRaisesRegex(RuntimeError, "daemon stopped"):
                with sampler:
                    sampler.thread.join(timeout=2)
            self.assertFalse(sampler.thread.is_alive())
            self.assertTrue(sampler.log.closed)

    def test_samples_are_saved_and_cleanup_preserves_client_error(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = MemorySampler("unused", "server", Path(directory) / "memory.jsonl")
            payload = {"read": "2026-11-10T12:00:00Z",
                       "memory_stats": {"usage": 100, "stats": {"inactive_file": 20}}}
            with patch.object(sampler.connection, "request"), patch.object(sampler.connection, "getresponse") as response:
                response.return_value.status = 200
                response.return_value.read.return_value = json.dumps(payload).encode()
                with self.assertRaisesRegex(ValueError, "client failed"):
                    with sampler:
                        raise ValueError("client failed")
            self.assertFalse(sampler.thread.is_alive())
            samples = [json.loads(line) for line in sampler.log_path.read_text().splitlines()]
            self.assertGreaterEqual(len(samples), 1)
            self.assertEqual(samples[0]["working_set_bytes"], 80)

    def test_summary_retains_missing_legacy_data_and_reports_max_peak(self):
        self.assertEqual(summarize_memory([{}, {}]),
                         {"ram_repeats": 0, "ram_avg_bytes_median": None, "ram_peak_bytes_max": None})
        self.assertEqual(summarize_memory([{}, {"ram_avg_bytes": 10, "ram_peak_bytes": 90},
                                             {"ram_avg_bytes": 30, "ram_peak_bytes": 40}]),
                         {"ram_repeats": 2, "ram_avg_bytes_median": 20, "ram_peak_bytes_max": 90})


if __name__ == "__main__":
    unittest.main()
