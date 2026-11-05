import io
import json
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from helpers import WarehouseTest

from trustflow.cli import main
from trustflow.demo import generate
from trustflow.errors import PipelineError


class CliTests(WarehouseTest):
    def invoke(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = main(args)
        return status, out.getvalue(), err.getvalue()

    def test_demo_to_report_and_replay_work_without_network(self):
        with patch(
            "socket.socket",
            side_effect=AssertionError("Network is forbidden in offline tests"),
        ):
            demo = self.path / "demo"
            status, out, _ = self.invoke(["generate-demo", "--count", "50", "--output", str(demo)])
            self.assertEqual(status, 0)
            expected = json.loads(out)
            args = ["ingest", str(demo), "--data-dir", str(self.root)]
            status, out, _ = self.invoke(args)
            self.assertEqual(status, 0)
            self.assertEqual(json.loads(out)["inserted_rows"], expected["events"])
            status, out, _ = self.invoke(args)
            self.assertEqual(status, 0)
            self.assertEqual(json.loads(out)["duplicate_rows"], expected["events"])
            status, out, _ = self.invoke(
                [
                    "report",
                    "--data-dir",
                    str(self.root),
                    "--as-of",
                    expected["suggested_as_of"],
                    "--output",
                    str(self.path / "report"),
                ]
            )
            self.assertEqual(status, 0)
            self.assertEqual(json.loads(out)["totals"]["requested_count"], 50)

    def test_seed_produces_identical_partition_bytes(self):
        left, right = self.path / "left", self.path / "right"
        generate(left, 100, 42)
        generate(right, 100, 42)
        self.assertEqual(
            {p.name: p.read_bytes() for p in left.iterdir()},
            {p.name: p.read_bytes() for p in right.iterdir()},
        )

    def test_generator_refuses_to_overwrite(self):
        target = self.path / "demo"
        generate(target, 10)
        with self.assertRaises(PipelineError):
            generate(target, 10)

    def test_missing_input_returns_machine_readable_error(self):
        status, out, err = self.invoke(
            ["ingest", str(self.path / "absent"), "--data-dir", str(self.root)]
        )
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err)["code"], "IO_ERROR")

    def test_empty_directory_and_invalid_count_fail(self):
        empty = self.path / "empty"
        empty.mkdir()
        status, _, err = self.invoke(["ingest", str(empty), "--data-dir", str(self.root)])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(err)["code"], "NO_FILES")
        status, _, err = self.invoke(["generate-demo", "--count", "0", "--output", str(empty)])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(err)["code"], "INVALID_COUNT")
