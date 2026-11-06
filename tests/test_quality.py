import io
import json
from contextlib import redirect_stderr, redirect_stdout

from helpers import WarehouseTest, event

from trustflow.cli import main
from trustflow.errors import PipelineError
from trustflow.pipeline import ingest
from trustflow.quality import evaluate, load_policy
from trustflow.report import report


class QualityTests(WarehouseTest):
    def policy(self, rules, kind="synthetic"):
        path = self.path / "policy.toml"
        path.write_text(f'[policy]\nversion = 1\ndataset_kind = "{kind}"\n[rules]\n{rules}\n')
        return path

    def invoke(self, args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(args)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_explicit_quality_policy_passes(self):
        ingest(self.write([event()]), self.root)
        policy = load_policy(self.policy("min_requested_count = 1\nmax_orphan_workflows = 0"))
        self.assertEqual(
            evaluate(report(self.root, "2025-01-02T00:00:00Z"), policy)["status"], "passed"
        )

    def test_insufficient_population_is_failure_not_false_success(self):
        ingest(self.write([event()]), self.root)
        policy = load_policy(self.policy("min_sla_met_pct = 95.0"))
        result = evaluate(report(self.root, "2025-01-01T00:01:00Z"), policy)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["checks"][0]["reason"], "no_eligible_population")

    def test_invalid_unknown_nonfinite_boolean_and_empty_policy_rejected(self):
        for rules in [
            "unknown_rule = 1",
            "min_sla_met_pct = nan",
            "max_running_batches = true",
            "max_orphan_workflows = -1",
            "min_sla_met_pct = 101",
            "",
        ]:
            with self.subTest(rules=rules), self.assertRaises(PipelineError):
                load_policy(self.policy(rules))

    def test_operational_policy_cannot_grade_synthetic_data(self):
        ingest(self.write([event()]), self.root)
        policy = load_policy(self.policy("min_requested_count = 1", "operational"))
        with self.assertRaises(PipelineError):
            evaluate(report(self.root, "2025-01-02T00:00:00Z"), policy)

    def test_failed_policy_returns_exit_three_and_json_evidence(self):
        ingest(self.write([event()]), self.root)
        policy = self.policy("max_overdue_pending_count = 0")
        code, stdout, _ = self.invoke(
            [
                "check",
                "--data-dir",
                str(self.root),
                "--as-of",
                "2025-01-02T00:00:00Z",
                "--policy",
                str(policy),
            ]
        )
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(stdout)["status"], "failed")

    def test_continue_on_error_processes_later_files_and_reports_failure(self):
        directory = self.path / "inputs"
        directory.mkdir()
        (directory / "01.ndjson").write_text("broken")
        (directory / "02.ndjson").write_bytes(self.write([event()]).read_bytes())
        code, stdout, _ = self.invoke(
            ["ingest", str(directory), "--data-dir", str(self.root), "--continue-on-error"]
        )
        self.assertEqual(code, 1)
        result = json.loads(stdout)
        self.assertEqual(result["failed_batches"], 1)
        self.assertEqual(result["inserted_rows"], 1)

    def test_audit_and_rebuild_entrypoints(self):
        ingest(self.write([event()]), self.root)
        code, stdout, _ = self.invoke(["audit", "--data-dir", str(self.root)])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["status"], "passed")
        code, stdout, _ = self.invoke(
            ["rebuild", "--data-dir", str(self.root), "--destination", str(self.path / "rebuilt")]
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["status"], "rebuilt")
