import hashlib
import json
from unittest.mock import patch
from uuid import uuid4

from helpers import WarehouseTest, event

from trustflow.errors import PipelineError
from trustflow.pipeline import ingest


class PipelineTests(WarehouseTest):
    def test_preserves_exact_bytes_and_audit_lineage(self):
        self.write([event(), event(kind="signed", at="2025-01-01T00:10:00Z")])
        original = self.source.read_bytes()
        result = ingest(self.source, self.root)
        self.assertEqual((self.root / result["raw_key"]).read_bytes(), original)
        self.assertEqual(result["raw_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(
            self.query("SELECT DISTINCT source_batch_id FROM events"),
            [(result["batch_id"],)],
        )

    def test_reprocessing_is_idempotent(self):
        self.write([event(), event(kind="signed", at="2025-01-01T00:10:00Z")])
        first = ingest(self.source, self.root)
        before = self.query("SELECT * FROM events ORDER BY event_id")
        second = ingest(self.source, self.root)
        self.assertEqual(second["inserted_rows"], 0)
        self.assertEqual(second["duplicate_rows"], 2)
        self.assertEqual(before, self.query("SELECT * FROM events ORDER BY event_id"))
        self.assertEqual(first["raw_sha256"], second["raw_sha256"])
        self.assertEqual(len(list((self.root / "bronze").rglob("*.ndjson"))), 1)

    def test_semantic_duplicate_with_equivalent_timezone(self):
        ingest(self.write([event()]), self.root)
        result = ingest(self.write([event(at="2024-12-31T21:00:00-03:00")]), self.root)
        self.assertEqual(result["duplicate_rows"], 1)

    def test_duplicates_within_a_batch(self):
        result = ingest(self.write([event(), event()]), self.root)
        self.assertEqual((result["inserted_rows"], result["duplicate_rows"]), (1, 1))

    def test_conflicting_event_id_rolls_back_entire_batch(self):
        ingest(self.write([event()]), self.root)
        with self.assertRaises(PipelineError) as error:
            ingest(self.write([event(flow=2), event(channel="mobile")]), self.root)
        self.assertEqual(error.exception.code, "EVENT_ID_CONFLICT")
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(1,)])
        self.assertEqual(
            self.query("SELECT inserted_rows FROM batches WHERE status='rejected'"),
            [(0,)],
        )
        receipt = json.loads(next((self.root / "quarantine").glob("*.json")).read_text())
        self.assertTrue((self.root / receipt["raw_key"]).exists())

    def test_same_batch_id_collision_is_rejected(self):
        with self.assertRaises(PipelineError):
            ingest(self.write([event(), event(channel="web")]), self.root)
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(0,)])

    def test_late_request_reconciles_terminal_event(self):
        ingest(self.write([event(kind="signed", at="2025-01-01T01:00:00Z")]), self.root)
        self.assertEqual(
            self.query("SELECT status FROM workflow_lifecycle"), [("awaiting_request",)]
        )
        ingest(self.write([event()]), self.root)
        self.assertEqual(self.query("SELECT status FROM workflow_lifecycle"), [("signed",)])

    def test_multiple_terminal_states_reject_new_batch(self):
        ingest(
            self.write([event(), event(kind="signed", at="2025-01-01T01:00:00Z")]),
            self.root,
        )
        with self.assertRaises(PipelineError) as error:
            ingest(self.write([event(kind="failed", at="2025-01-01T02:00:00Z")]), self.root)
        self.assertEqual(error.exception.code, "LIFECYCLE_CONFLICT")
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(2,)])

    def test_multiple_requests_with_different_ids_are_not_silent_duplicates(self):
        with self.assertRaises(PipelineError):
            ingest(self.write([event(), event(event_id=str(uuid4()))]), self.root)
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(0,)])

    def test_invalid_event_time_order_rejects_late_request(self):
        ingest(self.write([event(kind="signed")]), self.root)
        with self.assertRaises(PipelineError) as error:
            ingest(self.write([event(at="2025-01-01T01:00:00Z")]), self.root)
        self.assertEqual(error.exception.code, "TEMPORAL_ORDER")
        self.assertEqual(
            self.query("SELECT status FROM workflow_lifecycle"), [("awaiting_request",)]
        )

    def test_dimension_conflict_is_rejected(self):
        with self.assertRaises(PipelineError) as error:
            ingest(self.write([event(), event(kind="signed", channel="web")]), self.root)
        self.assertEqual(error.exception.code, "DIMENSION_CONFLICT")

    def test_invalid_json_utf8_empty_and_blank_lines_are_quarantined(self):
        for body in [b"{broken}", b"\xff", b"", b"\n"]:
            with self.subTest(body=body):
                self.source.write_bytes(body)
                with self.assertRaises(PipelineError):
                    ingest(self.source, self.root)
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(0,)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM batches WHERE status='rejected'"), [(4,)])

    def test_malformed_later_row_does_not_load_earlier_valid_rows(self):
        self.write([event()])
        with self.source.open("a") as handle:
            handle.write("not-json\n")
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root)
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(0,)])
        self.assertEqual(self.query("SELECT input_rows FROM batches"), [(2,)])

    def test_detects_bronze_corruption_without_overwriting(self):
        result = ingest(self.write([event()]), self.root)
        raw = self.root / result["raw_key"]
        raw.write_bytes(b"corrupted")
        with self.assertRaises(PipelineError) as error:
            ingest(self.source, self.root)
        self.assertEqual(error.exception.code, "RAW_INTEGRITY")
        self.assertEqual(raw.read_bytes(), b"corrupted")

    def test_separates_synthetic_and_operational_warehouses(self):
        ingest(self.write([event()]), self.root)
        with self.assertRaises(PipelineError) as error:
            ingest(self.source, self.root, dataset_kind="operational")
        self.assertEqual(error.exception.code, "CONFIG_MISMATCH")

    def test_sla_changes_require_separate_warehouse(self):
        ingest(self.write([event()]), self.root)
        with self.assertRaises(PipelineError):
            ingest(self.source, self.root, sla_seconds=1800)

    def test_size_and_row_limits_fail_explicitly(self):
        self.write([event(), event()])
        with (
            patch("trustflow.storage.MAX_BYTES", 10),
            self.assertRaises(PipelineError) as error,
        ):
            ingest(self.source, self.root)
        self.assertEqual(error.exception.code, "BATCH_TOO_LARGE")
        with (
            patch("trustflow.pipeline.MAX_ROWS", 1),
            self.assertRaises(PipelineError) as error,
        ):
            ingest(self.source, self.root)
        self.assertEqual(error.exception.code, "ROW_COUNT")
