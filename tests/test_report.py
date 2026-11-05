import csv
import json

from helpers import WarehouseTest, event

from trustflow.errors import PipelineError
from trustflow.pipeline import ingest
from trustflow.report import export_report, report


class ReportTests(WarehouseTest):
    def test_known_cohort_metrics_denominators_and_nearest_rank_p95(self):
        events = [event(flow=i) for i in range(1, 7)]
        events += [
            event(flow=7, at="2025-01-01T23:30:00Z"),
            event(flow=1, kind="signed", at="2025-01-01T00:30:00Z"),
            event(flow=2, kind="signed", at="2025-01-01T02:00:00Z"),
            event(flow=3, kind="failed", at="2025-01-01T00:10:00Z"),
            event(flow=5, kind="canceled", at="2025-01-01T00:10:00Z"),
            event(flow=6, kind="expired", at="2025-01-01T01:00:00Z"),
        ]
        ingest(self.write(events), self.root)
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(
            result["totals"],
            {
                "requested_count": 7,
                "signed_count": 2,
                "failed_count": 1,
                "canceled_count": 1,
                "expired_count": 1,
                "pending_count": 2,
                "overdue_pending_count": 1,
                "sla_due_count": 6,
                "sla_met_count": 1,
                "sla_met_pct": 16.67,
            },
        )
        self.assertEqual(result["cohorts"][0]["avg_signed_seconds"], 4500)
        self.assertEqual(result["cohorts"][0]["p95_signed_seconds"], 7200)
        self.assertEqual(
            result["failure_events_by_code"],
            [{"failure_code": "TIMEOUT", "event_count": 1}],
        )

    def test_event_time_snapshot_excludes_future_outcomes_and_requests(self):
        ingest(
            self.write(
                [
                    event(),
                    event(kind="signed", at="2025-01-03T00:00:00Z"),
                    event(flow=2, at="2025-01-04T00:00:00Z"),
                ]
            ),
            self.root,
        )
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(result["totals"]["requested_count"], 1)
        self.assertEqual(result["totals"]["signed_count"], 0)
        self.assertEqual(result["totals"]["pending_count"], 1)

    def test_sla_boundary_includes_exact_deadline_but_not_one_microsecond_late(self):
        ingest(
            self.write(
                [
                    event(),
                    event(flow=2),
                    event(kind="signed", at="2025-01-01T01:00:00Z"),
                    event(flow=2, kind="signed", at="2025-01-01T01:00:00.000001Z"),
                ]
            ),
            self.root,
        )
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(result["totals"]["sla_due_count"], 2)
        self.assertEqual(result["totals"]["sla_met_count"], 1)

    def test_fresh_requests_are_not_due_and_zero_denominator_is_null(self):
        ingest(self.write([event()]), self.root)
        result = report(self.root, "2025-01-01T00:30:00Z")
        self.assertEqual(result["totals"]["sla_due_count"], 0)
        self.assertIsNone(result["totals"]["sla_met_pct"])
        self.assertIsNone(result["cohorts"][0]["p95_signed_seconds"])

    def test_orphan_terminal_is_visible_but_not_falsely_counted_as_a_request(self):
        ingest(self.write([event(kind="failed")]), self.root)
        result = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(result["quality"]["workflows_awaiting_request"], 1)
        self.assertEqual(result["totals"]["requested_count"], 0)
        ingest(self.write([event()]), self.root)
        revised = report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(revised["quality"]["workflows_awaiting_request"], 0)
        self.assertEqual(revised["totals"]["failed_count"], 1)

    def test_channel_filter_applies_to_metrics_and_quality(self):
        ingest(
            self.write(
                [
                    event(),
                    event(flow=2, channel="web"),
                    event(flow=3, kind="failed", channel="web"),
                ]
            ),
            self.root,
        )
        result = report(self.root, "2025-01-02T00:00:00Z", channel="api")
        self.assertEqual(result["totals"]["requested_count"], 1)
        self.assertEqual(result["quality"]["workflows_awaiting_request"], 0)
        self.assertEqual(result["failure_events_by_code"], [])

    def test_empty_as_of_and_export(self):
        ingest(self.write([event()]), self.root)
        result = report(self.root, "2024-12-31T00:00:00Z")
        output = self.path / "reports"
        export_report(result, output)
        self.assertEqual(json.loads((output / "report.json").read_text()), result)
        with (output / "cohorts.csv").open() as handle:
            reader = csv.DictReader(handle)
            self.assertIn("p95_signed_seconds", reader.fieldnames)
            self.assertEqual(list(reader), [])

    def test_report_is_read_only(self):
        ingest(self.write([event()]), self.root)
        before = (self.root / "warehouse.sqlite").read_bytes()
        report(self.root, "2025-01-02T00:00:00Z")
        self.assertEqual(before, (self.root / "warehouse.sqlite").read_bytes())

    def test_missing_warehouse_does_not_create_one(self):
        with self.assertRaises(PipelineError):
            report(self.root, "2025-01-02T00:00:00Z")
        self.assertFalse(self.root.exists())
