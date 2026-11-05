import unittest

from helpers import event

from trustflow.contract import epoch_microseconds, event_hash, validate_event
from trustflow.errors import PipelineError
from trustflow.pipeline import parse_batch


class ContractTests(unittest.TestCase):
    def test_normalizes_timezone_and_hash(self):
        utc = validate_event(event(), "synthetic")
        local = validate_event(event(at="2024-12-31T21:00:00-03:00"), "synthetic")
        self.assertEqual(utc, local)
        self.assertEqual(event_hash(utc), event_hash(local))

    def test_epoch_microseconds_are_exact(self):
        value = validate_event(event(at="1970-01-01T00:00:00.000001Z"), "synthetic")
        self.assertEqual(epoch_microseconds(value["occurred_at"]), 1)

    def test_rejects_missing_extra_or_non_object_fields(self):
        missing = event()
        del missing["channel"]
        for payload in [missing, event(email="never-store@example.invalid"), [], None]:
            with (
                self.subTest(payload=type(payload).__name__),
                self.assertRaises(PipelineError),
            ):
                validate_event(payload, "synthetic")

    def test_rejects_invalid_versions(self):
        for version in [True, "1", 2, 1.0, None]:
            with self.subTest(version=version), self.assertRaises(PipelineError):
                validate_event(event(schema_version=version), "synthetic")

    def test_rejects_invalid_timestamps(self):
        for value in [
            "2025-01-01",
            "2025-01-01T00:00:00",
            "2025-02-30T00:00:00Z",
            "2025-01-01T00:00:00+01:99",
            "2025-01-01T00:00:00+24:00",
            None,
            1,
        ]:
            with self.subTest(value=value), self.assertRaises(PipelineError):
                validate_event(event(at=value), "synthetic")

    def test_rejects_invalid_enums_and_ids(self):
        for change in [
            {"channel": "email"},
            {"event_type": "unknown"},
            {"document_type": []},
            {"workflow_id": "customer-123"},
            {"event_id": None},
            {"dataset_kind": "operational"},
        ]:
            with self.subTest(change=change), self.assertRaises(PipelineError):
                validate_event(event(**change), "synthetic")

    def test_failure_code_is_required_only_for_failed(self):
        for payload in [
            event(kind="failed", failure_code=None),
            event(failure_code="TIMEOUT"),
            event(kind="failed", failure_code="SENSITIVE_FREE_TEXT"),
        ]:
            with self.assertRaises(PipelineError):
                validate_event(payload, "synthetic")

    def test_error_does_not_echo_unknown_input_values(self):
        with self.assertRaises(PipelineError) as error:
            validate_event(event(channel="private-example-value"), "synthetic")
        self.assertNotIn("private-example-value", str(error.exception))

    def test_rejects_duplicate_json_keys(self):
        with self.assertRaises(PipelineError) as error:
            parse_batch(b'{"channel":"web","channel":"api"}', "synthetic")
        self.assertEqual(error.exception.code, "DUPLICATE_JSON_KEY")
