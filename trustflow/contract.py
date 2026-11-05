"""Versioned allowlist contract. Never log input values on validation errors."""

import hashlib
import json
import re
from datetime import UTC, datetime
from uuid import UUID

from .errors import PipelineError

FIELDS = {
    "schema_version",
    "event_id",
    "workflow_id",
    "event_type",
    "occurred_at",
    "channel",
    "document_type",
    "failure_code",
    "dataset_kind",
}
EVENT_TYPES = {"requested", "signed", "failed", "canceled", "expired"}
CHANNELS = {"web", "mobile", "api"}
DOCUMENT_TYPES = {"contract", "authorization", "service_order"}
FAILURE_CODES = {"PROVIDER_UNAVAILABLE", "INVALID_DOCUMENT", "TIMEOUT"}
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")


def utc_timestamp(value: str) -> str:
    if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
        raise PipelineError("INVALID_TIMESTAMP", "Timestamp must include time and UTC offset.")
    try:
        if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(UTC).isoformat(timespec="microseconds")
    except (ValueError, OverflowError) as exc:
        raise PipelineError("INVALID_TIMESTAMP", "Invalid calendar date or UTC offset.") from exc


def epoch_microseconds(value: str) -> int:
    delta = datetime.fromisoformat(value) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def validate_event(value: object, dataset_kind: str) -> dict:
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise PipelineError("SCHEMA_FIELDS", "Event fields do not match schema version 1.")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise PipelineError("SCHEMA_VERSION", "Only integer schema_version 1 is supported.")
    event = dict(value)
    for field in ("event_id", "workflow_id"):
        try:
            if not isinstance(event[field], str) or str(UUID(event[field])) != event[field]:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise PipelineError("INVALID_ID", f"{field} must be a canonical UUID.") from exc
    for field, allowed in (
        ("event_type", EVENT_TYPES),
        ("channel", CHANNELS),
        ("document_type", DOCUMENT_TYPES),
        ("dataset_kind", {dataset_kind}),
    ):
        if not isinstance(event[field], str) or event[field] not in allowed:
            raise PipelineError("INVALID_ENUM", f"Invalid {field}.")
    failure = event["failure_code"]
    if event["event_type"] == "failed":
        if not isinstance(failure, str) or failure not in FAILURE_CODES:
            raise PipelineError(
                "INVALID_FAILURE_CODE", "Failed events require a known failure code."
            )
    elif failure is not None:
        raise PipelineError("INVALID_FAILURE_CODE", "Only failed events may have a failure code.")
    event["occurred_at"] = utc_timestamp(event["occurred_at"])
    return event


def event_hash(event: dict) -> str:
    return hashlib.sha256(canonical_json(event).encode("utf-8")).hexdigest()
