import hashlib
import json
import sqlite3
from contextlib import closing
from unittest.mock import patch

from helpers import WarehouseTest, event

from trustflow.errors import PipelineError
from trustflow.export import export_report, verify_export
from trustflow.pipeline import ingest
from trustflow.report import report


class SnapshotTests(WarehouseTest):
    def timed_ingest(self, values, accepted_at):
        with patch("trustflow.pipeline.now_utc", return_value=accepted_at):
            return ingest(self.write(values), self.root)

    def test_known_at_preserves_snapshot_after_late_event_and_replay(self):
        self.timed_ingest([event()], "2025-01-01T00:01:00.000000+00:00")
        original = report(self.root, "2025-01-02T00:00:00Z", known_at="2025-01-01T01:00:00Z")
        self.timed_ingest(
            [event(kind="signed", at="2025-01-01T00:30:00Z")], "2025-01-01T03:00:00.000000+00:00"
        )
        self.timed_ingest([event()], "2025-01-01T04:00:00.000000+00:00")
        past = report(self.root, "2025-01-02T00:00:00Z", known_at="2025-01-01T01:00:00Z")
        latest = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(original["totals"], past["totals"])
        self.assertEqual(original["event_snapshot_sha256"], past["event_snapshot_sha256"])
        self.assertEqual(past["totals"]["pending_count"], 1)
        self.assertEqual(latest["totals"]["signed_count"], 1)

    def test_known_at_boundary_and_empty_population(self):
        self.timed_ingest([event()], "2025-01-01T00:01:00.000000+00:00")
        before = report(self.root, "2025-01-02T00:00:00Z", known_at="2025-01-01T00:00:59Z")
        exact = report(self.root, "2025-01-02T00:00:00Z", known_at="2025-01-01T00:01:00Z")
        self.assertEqual(before["source_event_count"], 0)
        self.assertEqual(exact["source_event_count"], 1)

    def test_backlog_age_boundaries_are_disjoint_and_reconcile(self):
        ingest(
            self.write(
                [
                    event(flow=1, at="2025-01-01T23:30:00Z"),
                    event(flow=2, at="2025-01-01T23:00:00Z"),
                    event(flow=3, at="2025-01-01T20:00:00Z"),
                    event(flow=4, at="2025-01-01T00:00:00Z"),
                ]
            ),
            self.root,
        )
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(
            [r["bucket"] for r in result["backlog"]],
            ["within_sla", "one_to_four_sla", "four_to_twenty_four_sla", "over_twenty_four_sla"],
        )
        self.assertEqual([r["pending_count"] for r in result["backlog"]], [1, 1, 1, 1])
        self.assertEqual(
            sum(r["pending_count"] for r in result["backlog"]), result["totals"]["pending_count"]
        )

    def test_segment_reconciliation_document_filter_and_zero_completed_counts(self):
        ingest(
            self.write([event(), event(flow=2, channel="web", document_type="authorization")]),
            self.root,
        )
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(sum(row["requested_count"] for row in result["segments"]), 2)
        self.assertTrue(
            all(row["signed_count"] == row["failed_count"] == 0 for row in result["segments"])
        )
        filtered = report(self.root, "2025-01-02T00:00:00Z", document_type="authorization")
        self.assertEqual(filtered["totals"]["requested_count"], 1)
        self.assertEqual(filtered["segments"][0]["channel"], "web")

    def test_report_bundle_is_idempotent_and_hashes_cover_all_data_files(self):
        ingest(self.write([event()]), self.root)
        result = report(self.root, "2025-01-02T00:00:00Z")
        output = self.path / "report"
        export_report(result, output)
        export_report(result, output)
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(
            set(manifest["files"]), {"report.json", "cohorts.csv", "segments.csv", "backlog.csv"}
        )
        for name, expected in manifest["files"].items():
            self.assertEqual(hashlib.sha256((output / name).read_bytes()).hexdigest(), expected)

    def test_different_export_never_overwrites_previous_evidence(self):
        ingest(self.write([event()]), self.root)
        output = self.path / "report"
        export_report(report(self.root, "2025-01-02T00:00:00Z"), output)
        before = (output / "report.json").read_bytes()
        with self.assertRaises(PipelineError):
            export_report(report(self.root, "2025-01-03T00:00:00Z"), output)
        self.assertEqual(before, (output / "report.json").read_bytes())

    def test_export_failure_does_not_publish_a_partial_bundle(self):
        ingest(self.write([event()]), self.root)
        output = self.path / "report"
        with patch("trustflow.export.os.fsync", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                export_report(report(self.root, "2025-01-02T00:00:00Z"), output)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.path.glob(".trustflow-export-*")), [])

    def test_export_verifier_detects_modified_and_missing_files(self):
        ingest(self.write([event()]), self.root)
        output = self.path / "report"
        export_report(report(self.root, "2025-01-02T00:00:00Z"), output)
        self.assertEqual(verify_export(output)["status"], "passed")
        (output / "cohorts.csv").write_text("changed")
        (output / "backlog.csv").unlink()
        result = verify_export(output)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(set(result["failures"]), {"cohorts.csv", "backlog.csv"})

    def test_export_verifier_refuses_manifest_path_traversal(self):
        output = self.path / "report"
        output.mkdir()
        (output / "manifest.json").write_text(
            json.dumps({"manifest_version": 1, "files": {"../outside": "abc"}})
        )
        with self.assertRaises(PipelineError):
            verify_export(output)

    def test_staging_does_not_issue_select_per_event(self):
        from trustflow.storage import connect

        statements = []

        def traced(*args, **kwargs):
            db = connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            return db

        with patch("trustflow.pipeline.connect", side_effect=traced):
            ingest(self.write([event(flow=i) for i in range(1000)]), self.root)
        selects = [sql for sql in statements if sql.lstrip().upper().startswith(("SELECT", "WITH"))]
        self.assertLess(len(selects), 10)
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(1000,)])

    def test_readonly_report_preserves_legacy_warehouse_without_new_indexes(self):
        ingest(self.write([event()]), self.root)
        with closing(sqlite3.connect(self.root / "warehouse.sqlite")) as db, db:
            db.execute("DROP INDEX idx_events_batch")
            db.execute("DROP INDEX idx_batches_raw_status")
        before = (self.root / "warehouse.sqlite").read_bytes()
        self.assertEqual(report(self.root, "2025-01-02T00:00:00Z")["totals"]["requested_count"], 1)
        self.assertEqual(before, (self.root / "warehouse.sqlite").read_bytes())
