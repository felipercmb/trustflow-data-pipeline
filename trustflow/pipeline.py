"""Atomic ingestion: an invalid batch changes no analytical events."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .contract import epoch_microseconds, event_hash, validate_event
from .errors import PipelineError
from .storage import connect, preserve_raw, read_bounded

MAX_ROWS = 50_000


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def reject_duplicate_keys(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError("DUPLICATE_JSON_KEY", "JSON object contains a repeated key.")
        result[key] = value
    return result


def parse_batch(body: bytes, dataset_kind: str) -> list[dict]:
    try:
        lines = body.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise PipelineError("INVALID_ENCODING", "Batch must use UTF-8.") from exc
    if not lines or len(lines) > MAX_ROWS:
        raise PipelineError("ROW_COUNT", "Batch must contain 1 to 50,000 JSON lines.")
    events = []
    for index, line in enumerate(lines, start=1):
        try:
            payload = json.loads(line, object_pairs_hook=reject_duplicate_keys)
            events.append(validate_event(payload, dataset_kind))
        except json.JSONDecodeError as exc:
            raise PipelineError("INVALID_JSON", f"Line {index}: invalid JSON.") from exc
        except PipelineError as exc:
            raise PipelineError(exc.code, f"Line {index}: {exc}") from exc
    return events


def validate_lifecycles(db: sqlite3.Connection, workflow_ids: set[str]) -> None:
    for workflow_id in workflow_ids:
        rows = db.execute("SELECT * FROM events WHERE workflow_id = ?", (workflow_id,)).fetchall()
        requests = [row for row in rows if row["event_type"] == "requested"]
        terminals = [row for row in rows if row["event_type"] != "requested"]
        if len(requests) > 1 or len(terminals) > 1:
            raise PipelineError(
                "LIFECYCLE_CONFLICT",
                "A workflow permits one request and at most one terminal event.",
            )
        if len({(row["channel"], row["document_type"]) for row in rows}) > 1:
            raise PipelineError("DIMENSION_CONFLICT", "Workflow dimensions changed across events.")
        if requests and terminals and terminals[0]["occurred_at"] < requests[0]["occurred_at"]:
            raise PipelineError(
                "TEMPORAL_ORDER", "Terminal event precedes its request in event time."
            )


def ingest(
    source: Path, root: Path, dataset_kind: str = "synthetic", sla_seconds: int = 3600
) -> dict:
    body = read_bounded(source)
    with closing(connect(root, dataset_kind, sla_seconds)) as db:
        digest, key = preserve_raw(root, body)
        batch_id = str(uuid4())
        with db:
            db.execute(
                "INSERT INTO batches(batch_id, raw_sha256, raw_key, started_at, status) VALUES (?, ?, ?, ?, 'running')",
                (batch_id, digest, key, now_utc()),
            )
        total = len(body.splitlines())
        inserted = duplicates = 0
        try:
            events = parse_batch(body, dataset_kind)
            total = len(events)
            with db:
                db.execute("BEGIN IMMEDIATE")
                for event in events:
                    digest_event = event_hash(event)
                    existing = db.execute(
                        "SELECT payload_sha256 FROM events WHERE event_id = ?",
                        (event["event_id"],),
                    ).fetchone()
                    if existing:
                        if existing[0] != digest_event:
                            raise PipelineError(
                                "EVENT_ID_CONFLICT",
                                "Existing event_id has a different canonical payload.",
                            )
                        duplicates += 1
                        continue
                    db.execute(
                        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            event["event_id"],
                            event["workflow_id"],
                            event["event_type"],
                            event["occurred_at"],
                            epoch_microseconds(event["occurred_at"]),
                            event["channel"],
                            event["document_type"],
                            event["failure_code"],
                            digest_event,
                            batch_id,
                        ),
                    )
                    inserted += 1
                validate_lifecycles(db, {event["workflow_id"] for event in events})
                db.execute(
                    "UPDATE batches SET status='accepted', finished_at=?, input_rows=?, inserted_rows=?, duplicate_rows=? WHERE batch_id=?",
                    (now_utc(), total, inserted, duplicates, batch_id),
                )
        except (PipelineError, sqlite3.DatabaseError) as exc:
            code = exc.code if isinstance(exc, PipelineError) else "DATABASE_ERROR"
            message = str(exc) if isinstance(exc, PipelineError) else "Database transaction failed."
            with db:
                db.execute(
                    "UPDATE batches SET status='rejected', finished_at=?, input_rows=?, error_code=?, error_message=? WHERE batch_id=?",
                    (now_utc(), total, code, message, batch_id),
                )
            quarantine = root / "quarantine"
            quarantine.mkdir(exist_ok=True)
            (quarantine / f"{batch_id}.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "raw_key": key,
                        "raw_sha256": digest,
                        "error_code": code,
                        "message": message,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            raise PipelineError(code, f"Batch {batch_id} rejected: {message}") from exc
        return {
            "batch_id": batch_id,
            "status": "accepted",
            "raw_sha256": digest,
            "raw_key": key,
            "input_rows": total,
            "inserted_rows": inserted,
            "duplicate_rows": duplicates,
        }
