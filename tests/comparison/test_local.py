"""Protect comparison validity, immutable saved baselines, and server cleanup."""

import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import run_local as local


class LocalComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.settings = {**local.DEFAULTS, "clients": [1, 8], "seconds": 0.2, "repeats": 1}

    def samples(self, rps=100):
        return [{"database": "avalanche", "mode": mode, "clients": clients, "repeat": 1,
                 "seconds": 0.2, "keys": 1024, "value_bytes": 256,
                 "rps": rps, "completed": int(rps * 0.2),
                 "get": int(rps * 0.2) if mode == "get" else int(rps * 0.1) if mode == "mixed" else 0,
                 "set": int(rps * 0.2) if mode == "set" else int(rps * 0.1) if mode == "mixed" else 0,
                 "errors": 0, "drained": 0, "p50_us": 10, "p95_us": 20, "p99_us": 30}
                for mode in ["get", "set", "mixed"] for clients in [1, 8]]

    def saved_run(self, name="previous", status="success", format=local.FORMAT):
        directory = self.directory / name
        (directory / "bin").mkdir(parents=True)
        binary = directory / "bin/current-server"
        binary.write_bytes(b"saved binary")
        metadata = {"format": format, "status": status, "finished_utc": name,
                    "environment": {"target": "test-native"},
                    "variants": {"current": {"server_sha256": local.sha256(binary)}}}
        (directory / "metadata.json").write_text(json.dumps(metadata))
        return directory

    def test_rejects_incomplete_duplicate_failed_and_incomparable_samples(self):
        valid = self.samples()
        local.validate_samples(valid, self.settings)
        variants = [valid[:-1], valid + valid[:1]]
        for key, value in [("errors", 1), ("rps", float("nan")), ("rps", 999),
                           ("seconds", 10), ("value_bytes", 1), ("database", "redis"),
                           ("p99_us", 5), ("p99_us", float("inf")), ("completed", 0)]:
            rows = copy.deepcopy(valid)
            rows[0][key] = value
            variants.append(rows)
        for rows in variants:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                local.validate_samples(rows, self.settings)

    def test_summary_uses_medians_and_preserves_repeat_ranges(self):
        rows = [self.samples(rps)[0] for rps in [100, 200, 900]]
        self.assertEqual(local.summarize(rows)[0]["rps_median"], 200)
        self.assertEqual(local.summarize(rows)[0]["rps_min"], 100)
        self.assertEqual(local.summarize(rows)[0]["rps_max"], 900)

    def test_comparison_matches_connections_and_reports_signed_changes(self):
        before = local.summarize(self.samples(100))
        after = local.summarize(self.samples(150))
        after[0]["p99_us_median"] = 15
        rows = local.compare(before, list(reversed(after)))
        self.assertTrue(all(row["rps_change_percent"] == 50 for row in rows))
        changed = next(row for row in rows if row["mode"] == after[0]["mode"] and row["clients"] == after[0]["clients"])
        self.assertEqual(changed["p99_change_percent"], -50)
        with self.assertRaises(ValueError):
            local.compare(before[:-1], after)

    def test_saved_baseline_is_native_verified_and_read_only(self):
        directory = self.saved_run()
        before = {p: p.read_bytes() for p in directory.rglob("*") if p.is_file()}
        binary, provenance = local.saved_baseline(directory, {"target": "test-native"})
        self.assertEqual(binary.read_bytes(), b"saved binary")
        self.assertEqual(provenance["origin"], "saved local run")
        self.assertEqual(before, {p: p.read_bytes() for p in before})
        with self.assertRaisesRegex(ValueError, "target differs"):
            local.saved_baseline(directory, {"target": "different"})
        binary.write_bytes(b"changed binary")
        with self.assertRaisesRegex(ValueError, "binary changed"):
            local.saved_baseline(directory, {"target": "test-native"})

    def test_docker_results_and_failed_local_runs_are_not_baselines(self):
        for name, status, format in [("docker", "success", "docker"), ("failed", "failed", local.FORMAT)]:
            directory = self.saved_run(name, status, format)
            with self.assertRaisesRegex(ValueError, "successful native local run"):
                local.saved_baseline(directory, {"target": "test-native"})
        self.assertIsNone(local.previous_run({"target": "test-native"}, self.directory))
        oldest = self.saved_run("2026-01")
        newest = self.saved_run("2026-02")
        self.assertEqual(local.previous_run({"target": "test-native"}, self.directory), newest)
        self.assertIsNone(local.previous_run({"target": "other"}, self.directory))

    def test_only_one_local_runner_can_use_build_cache_and_host_at_a_time(self):
        with patch.object(local, "HERE", self.directory), local.exclusive_run():
            with self.assertRaisesRegex(RuntimeError, "another local benchmark"):
                with local.exclusive_run():
                    self.fail("concurrent benchmark acquired the lock")
        with patch.object(local, "HERE", self.directory), local.exclusive_run():
            pass

    @unittest.skipUnless(shutil.which("cargo") and shutil.which("rustc"), "requires a Rust toolchain")
    def test_build_recompiles_external_modules_when_switching_source_snapshots(self):
        first_source = self.directory / "first"
        package = first_source / "tests/comparison"
        (package / "src/bin").mkdir(parents=True)
        (first_source / "src").mkdir()
        (package / "Cargo.toml").write_text(
            '[package]\nname = "avalanche-comparison"\nversion = "0.1.0"\nedition = "2024"\n'
            '[[bin]]\nname = "bench-server"\npath = "src/bin/server.rs"\n'
        )
        (package / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "avalanche-comparison"\nversion = "0.1.0"\n'
        )
        (package / "src/bin/server.rs").write_text(
            '#[path = "../../../../src/application.rs"]\nmod application;\n'
            'fn main() { println!("{}", application::VALUE); }\n'
        )
        (first_source / "src/application.rs").write_text('pub const VALUE: &str = "before";\n')
        output = self.directory / "output"
        (output / "bin").mkdir(parents=True)
        (output / "logs").mkdir()
        target = next(line[6:] for line in local.capture(["rustc", "-Vv"]).splitlines()
                      if line.startswith("host: "))

        with patch.object(local, "HERE", self.directory / "harness"):
            local.build(first_source, "first", output, target)
            second_source = self.directory / "second"
            shutil.copytree(first_source, second_source)
            (second_source / "src/application.rs").write_text('pub const VALUE: &str = "after";\n')
            local.build(second_source, "second", output, target)

        self.assertEqual(local.capture([str(output / "bin/first-server")]), "before")
        self.assertEqual(local.capture([str(output / "bin/second-server")]), "after")

    def test_cleanup_runs_on_client_failure_and_keyboard_interrupt(self):
        for error in [RuntimeError("client failed"), KeyboardInterrupt()]:
            process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            try:
                with self.subTest(error=type(error).__name__), \
                     patch.object(local.subprocess, "Popen", return_value=process), \
                     patch.object(local, "wait_ready"), self.assertRaises(type(error)):
                    with local.running_server(Path("unused"), ("127.0.0.1", 1), 1, self.directory / "server.log"):
                        raise error
                self.assertIsNotNone(process.poll())
            finally:
                local.stop_process(process)

    @unittest.skipUnless(shutil.which("cargo") and shutil.which("rustc"), "requires a Rust toolchain")
    def test_root_server_uses_shared_library_and_release_profile_across_snapshots(self):
        source = self.directory / "first"
        (source / "src/bin").mkdir(parents=True)
        (source / "Cargo.toml").write_text(
            '[package]\nname = "avalanche"\nversion = "0.1.0"\nedition = "2024"\n'
            '[profile.release]\ndebug-assertions = true\n'
        )
        (source / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "avalanche"\nversion = "0.1.0"\n'
        )
        (source / "src/lib.rs").write_text('pub const VALUE: &str = "before";\n')
        (source / "src/bin/bench-server.rs").write_text(
            'fn main() { println!("{}:{}", avalanche::VALUE, cfg!(debug_assertions)); }\n'
        )
        output = self.directory / "output"
        (output / "bin").mkdir(parents=True)
        (output / "logs").mkdir()
        target = next(line[6:] for line in local.capture(["rustc", "-Vv"]).splitlines()
                      if line.startswith("host: "))
        with patch.object(local, "HERE", self.directory / "harness"):
            local.build(source, "first", output, target)
            second = self.directory / "second"
            shutil.copytree(source, second)
            (second / "src/lib.rs").write_text('pub const VALUE: &str = "after";\n')
            local.build(second, "second", output, target)
        self.assertEqual(local.capture([str(output / "bin/first-server")]), "before:true")
        self.assertEqual(local.capture([str(output / "bin/second-server")]), "after:true")

    def test_cli_presets_and_invalid_inputs(self):
        args = local.arguments(["--quick", "--repeats", "2", "--modes", "get"])
        self.assertEqual(args.clients, [1, 8])
        self.assertEqual(args.repeats, 2)
        self.assertEqual(args.modes, ["get"])
        self.assertEqual(args.seconds, 0.2)
        self.assertEqual(local.arguments(["--server-threads", "2"]).server_threads, 2)
        for flags in [["--seconds", "nan"], ["--seconds", "0"], ["--warmup-seconds", "-1"],
                      ["--clients", "1,1"], ["--clients", "0"], ["--clients", "x"],
                      ["--modes", "wrong"], ["--modes", "get,get"], ["--value-bytes", "-1"],
                      ["--server-threads", "0"], ["--baseline-ref", "HEAD", "--no-compare"]]:
            with self.subTest(flags=flags), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                local.arguments(flags)


if __name__ == "__main__":
    unittest.main()
