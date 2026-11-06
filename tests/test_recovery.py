import sqlite3
from contextlib import closing
from unittest.mock import patch

from helpers import WarehouseTest, event

from trustflow.audit import audit
from trustflow.errors import PipelineError
from trustflow.pipeline import ingest
from trustflow.recovery import rebuild
from trustflow.report import report


class RecoveryTests(WarehouseTest):
    def mutate(self, sql):
        with closing(sqlite3.connect(self.root / "warehouse.sqlite")) as db, db:
            db.execute(sql)

    def test_skip_accepted_does_not_create_another_attempt(self):
        first = ingest(self.write([event()]), self.root)
        second = ingest(self.source, self.root, skip_accepted=True)
        self.assertEqual(second["status"], "skipped")
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertEqual(self.query("SELECT COUNT(*) FROM batches"), [(1,)])

    def test_skipping_still_verifies_bronze_integrity(self):
        result = ingest(self.write([event()]), self.root)
        (self.root / result["raw_key"]).write_bytes(b"damaged")
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root, skip_accepted=True)

    def test_changed_source_is_processed_with_incremental_mode(self):
        ingest(self.write([event()]), self.root)
        result = ingest(self.write([event(flow=2)]), self.root, skip_accepted=True)
        self.assertEqual(result["inserted_rows"], 1)

    def test_rejected_content_is_never_silently_skipped(self):
        self.source.write_text("broken\n")
        for _ in range(2):
            with self.assertRaises(PipelineError):
                ingest(self.source, self.root, skip_accepted=True)
        self.assertEqual(self.query("SELECT COUNT(*) FROM batches"), [(2,)])

    def test_deep_json_is_quarantined_instead_of_leaving_running_batch(self):
        self.source.write_text("[" * 2000 + "0" + "]" * 2000)
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root)
        self.assertEqual(self.query("SELECT status FROM batches"), [("rejected",)])

    def test_audit_passes_with_duplicates_and_rejected_batches(self):
        ingest(self.write([event(), event()]), self.root)
        ingest(self.source, self.root)
        self.source.write_text("broken")
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root)
        result = audit(self.root)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["checked_events"], 1)
        self.assertEqual(result["checked_raw_files"], 2)

    def test_audit_detects_modified_silver_even_if_hash_column_is_unchanged(self):
        ingest(self.write([event()]), self.root)
        self.mutate("UPDATE events SET channel='web'")
        result = audit(self.root)
        self.assertEqual(result["status"], "failed")
        self.assertIn("EVENT_LINEAGE", {f["code"] for f in result["findings"]})

    def test_audit_detects_missing_raw_and_refuses_rebuild(self):
        first = ingest(self.write([event()]), self.root)
        (self.root / first["raw_key"]).unlink()
        self.assertEqual(audit(self.root)["status"], "failed")
        destination = self.path / "rebuilt"
        with self.assertRaises(PipelineError):
            rebuild(self.root, destination)
        self.assertFalse(destination.exists())

    def test_audit_rejects_unsafe_raw_address(self):
        ingest(self.write([event()]), self.root)
        self.mutate("UPDATE batches SET raw_key='../outside.ndjson'")
        self.assertEqual(audit(self.root)["findings"][0]["code"], "RAW_INTEGRITY")

    def test_rebuild_only_loads_accepted_content_and_preserves_business_metrics(self):
        ingest(self.write([event(kind="signed", at="2025-01-01T01:00:00Z")]), self.root)
        ingest(self.write([event()]), self.root)
        ingest(self.source, self.root)
        self.source.write_text("invalid")
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root)
        destination = self.path / "rebuilt"
        result = rebuild(self.root, destination)
        self.assertEqual(result["events"], 2)
        self.assertEqual(result["replayed_raw_files"], 2)
        self.assertEqual(audit(destination)["status"], "passed")
        self.assertEqual(
            report(self.root, "2025-01-02T00:00:00Z")["totals"],
            report(destination, "2025-01-02T00:00:00Z")["totals"],
        )

    def test_rebuild_never_overwrites_and_rejects_nested_destination(self):
        ingest(self.write([event()]), self.root)
        for destination in [self.root, self.root / "nested"]:
            with self.assertRaises(PipelineError):
                rebuild(self.root, destination)

    def test_failure_during_rebuild_does_not_publish_partial_warehouse(self):
        ingest(self.write([event()]), self.root)
        destination = self.path / "rebuilt"
        with patch("trustflow.recovery.ingest", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                rebuild(self.root, destination)
        self.assertFalse(destination.exists())
        self.assertEqual(list(self.path.glob(".trustflow-rebuild-*")), [])

    def test_running_batch_requires_investigation_before_rebuild(self):
        ingest(self.write([event()]), self.root)
        self.mutate(
            "INSERT INTO batches SELECT 'interrupted',raw_sha256,raw_key,started_at,NULL,'running',0,0,0,NULL,NULL FROM batches LIMIT 1"
        )
        with self.assertRaises(PipelineError):
            rebuild(self.root, self.path / "rebuilt")
