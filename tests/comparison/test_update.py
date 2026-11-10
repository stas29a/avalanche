"""Protect other databases and the previous report when updating measurements."""
import contextlib
import copy
import csv
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import run


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.run_directory = self.directory / "updates" / "new"
        (self.directory / "logs").mkdir()
        (self.run_directory / "logs").mkdir(parents=True)
        self.settings = {**run.PARAMETERS, "clients": [1, 8], "seconds": 0.2,
                         "warmup_seconds": 0, "repeats": 1, "databases": ["avalanche", "redis"]}
        self.metadata = {
            "settings": self.settings, "started_utc": "old-start", "finished_utc": "old-end",
            "client_image": {"Architecture": "arm64", "Id": "old-image"},
            "source_sha256": {"src/server.rs": "old-hash"},
            "databases": {name: {"version": f"old-{name}"} for name in self.settings["databases"]},
            "samples": 12, "failures": [],
        }
        self.old_rows = self.samples("avalanche", 100) + self.samples("redis", 200)
        (self.directory / "metadata.json").write_text(json.dumps(self.metadata))
        (self.directory / "raw.jsonl").write_text("".join(json.dumps(row) + "\n" for row in self.old_rows))
        (self.directory / "raw.csv").write_text("old csv")
        (self.directory / "report.html").write_text("old report")
        (self.directory / "logs" / "redis-server.log").write_text("old redis logs")
        (self.directory / "logs" / "avalanche-server.log").write_text("old avalanche logs")
        self.current = copy.deepcopy(self.metadata)
        self.current["settings"]["databases"] = ["avalanche"]
        self.current["databases"] = {"avalanche": {"version": "new-avalanche"}}
        self.current["source_sha256"] = {"src/server.rs": "new-hash"}
        self.current["client_image"]["Id"] = "new-image"
        self.current["finished_utc"] = "new-end"
        self.new_rows = self.samples("avalanche", 300)
        self.write_current()
        (self.run_directory / "logs" / "avalanche-server.log").write_text("new avalanche logs")
        self.original = run.snapshot(self.directory)

    def samples(self, database, rps):
        return [{"database": database, "mode": mode, "clients": clients, "repeat": 1,
                 "seconds": 0.2, "keys": self.settings["keys"], "value_bytes": self.settings["value_bytes"],
                 "rps": rps, "errors": 0}
                for mode in ["get", "set", "mixed"] for clients in self.settings["clients"]]

    def write_current(self):
        (self.run_directory / "metadata.json").write_text(json.dumps(self.current))
        (self.run_directory / "raw.jsonl").write_text("".join(json.dumps(row) + "\n" for row in self.new_rows))

    @staticmethod
    def render(command, **kwargs):
        directory = Path(command[-1])
        for name in run.REPORT_FILES:
            if name not in ["metadata.json", "raw.jsonl", "raw.csv"]:
                (directory / name).write_text("new generated report")
        return subprocess.CompletedProcess(command, 0)

    def merge(self):
        with contextlib.redirect_stdout(io.StringIO()):
            run.merge_update(self.directory, self.run_directory, self.original)

    def report_snapshot(self):
        return {str(path.relative_to(self.directory)): path.read_bytes()
                for path in self.directory.rglob("*")
                if path.is_file() and "updates" not in path.relative_to(self.directory).parts}

    def test_update_inherits_workload_and_rejects_incomparable_options(self):
        args = run.arguments(["--update", str(self.directory), "--databases", "avalanche", "--build"])
        for key in run.PARAMETERS:
            self.assertEqual(getattr(args, key), self.settings[key])
        for extra in [["--seconds", "10"], ["--quick"], ["--no-plots"], ["--clients", "32"],
                      ["--output", "other"], ["--databases", "valkey"]]:
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run.arguments(["--update", str(self.directory), "--databases", "avalanche", *extra])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            run.arguments(["--update", str(self.directory)])

    @patch.object(run.subprocess, "run", side_effect=render)
    def test_replaces_only_selected_database_and_archives_previous_report(self, render):
        self.merge()
        rows = [json.loads(line) for line in (self.directory / "raw.jsonl").read_text().splitlines()]
        self.assertEqual([row for row in rows if row["database"] == "redis"], self.samples("redis", 200))
        self.assertEqual([row for row in rows if row["database"] == "avalanche"], self.new_rows)
        metadata = json.loads((self.directory / "metadata.json").read_text())
        self.assertEqual(metadata["databases"]["redis"], self.metadata["databases"]["redis"])
        self.assertEqual(metadata["source_sha256"], self.metadata["source_sha256"])
        self.assertEqual(metadata["client_image"]["Id"], "old-image")
        self.assertEqual(metadata["databases"]["avalanche"]["source_sha256"], self.current["source_sha256"])
        self.assertEqual(metadata["samples"], 12)
        self.assertEqual(metadata["updates"][0]["databases"], ["avalanche"])
        self.assertEqual((self.directory / "logs" / "redis-server.log").read_text(), "old redis logs")
        self.assertEqual((self.directory / "logs" / "avalanche-server.log").read_text(), "new avalanche logs")
        for name, data in self.original.items():
            self.assertEqual((self.run_directory / "previous" / name).read_bytes(), data)
        self.assertEqual((self.run_directory / "previous" / "report.html").read_text(), "old report")
        render.assert_called_once()

    def test_incomplete_duplicate_or_failed_samples_leave_report_untouched(self):
        original_report = self.report_snapshot()
        variants = [self.new_rows[:-1], self.new_rows + self.new_rows[:1],
                    [{**row, "errors": 1} for row in self.new_rows],
                    [{**row, "seconds": 10} for row in self.new_rows]]
        for rows in variants:
            with self.subTest(rows=rows):
                self.new_rows = rows
                self.write_current()
                with self.assertRaises(ValueError):
                    self.merge()
                self.assertEqual(self.report_snapshot(), original_report)
        self.current["failures"] = [{"database": "avalanche", "error": "server stopped"}]
        self.write_current()
        with self.assertRaises(ValueError):
            self.merge()
        self.assertEqual(self.report_snapshot(), original_report)

    @patch.object(run.subprocess, "run", side_effect=render)
    def test_adding_ram_keeps_legacy_rows_empty_and_preserves_sample_logs(self, render):
        for row in self.new_rows:
            row.update(ram_avg_bytes=1024, ram_peak_bytes=2048, ram_samples=2)
        self.current["databases"]["avalanche"]["memory_measurement"] = {"interval_ms": 100}
        self.write_current()
        (self.run_directory / "logs" / "avalanche-get-1-memory.jsonl").write_text('{"usage_bytes": 2048}\n')
        self.merge()
        with (self.directory / "raw.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertTrue(all(row["ram_avg_bytes"] == "" for row in rows if row["database"] == "redis"))
        self.assertTrue(all(row["ram_avg_bytes"] == "1024" for row in rows if row["database"] == "avalanche"))
        metadata = json.loads((self.directory / "metadata.json").read_text())
        self.assertNotIn("memory_measurement", metadata["databases"]["redis"])
        self.assertEqual(metadata["databases"]["avalanche"]["memory_measurement"], {"interval_ms": 100})
        self.assertEqual((self.directory / "logs" / "avalanche-get-1-memory.jsonl").read_text(),
                         '{"usage_bytes": 2048}\n')

    def test_changed_report_is_not_overwritten(self):
        (self.directory / "raw.jsonl").write_text("another update")
        original_report = self.report_snapshot()
        with self.assertRaisesRegex(RuntimeError, "changed"):
            self.merge()
        self.assertEqual(self.report_snapshot(), original_report)

    @patch.object(run.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "plot.py"))
    def test_plot_failure_leaves_report_untouched(self, render):
        original_report = self.report_snapshot()
        with self.assertRaises(subprocess.CalledProcessError):
            self.merge()
        self.assertEqual(self.report_snapshot(), original_report)

    @patch.object(run.subprocess, "run", side_effect=render)
    def test_write_failure_restores_previous_files(self, render):
        original_report = self.report_snapshot()
        replace = Path.replace

        def fail_report_write(source, destination):
            if source.name == "report.html":
                raise OSError("simulated write failure")
            return replace(source, destination)

        with patch.object(Path, "replace", fail_report_write), self.assertRaises(OSError):
            self.merge()
        self.assertEqual(self.report_snapshot(), original_report)


if __name__ == "__main__":
    unittest.main()
