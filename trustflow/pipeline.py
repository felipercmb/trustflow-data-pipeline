"""Atomic ingestion: an invalid batch changes no analytical events."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .contract import epoch_microseconds, event_hash, validate_event
from .errors import PipelineError
from .storage import connect, preserve_raw, read_bounded, sql_text

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
        except (json.JSONDecodeError, RecursionError, ValueError) as exc:
            raise PipelineError("INVALID_JSON", f"Line {index}: invalid JSON.") from exc
        except PipelineError as exc:
            raise PipelineError(exc.code, f"Line {index}: {exc}") from exc
    return events


def load_staging(db: sqlite3.Connection, events: list[dict], batch_id: str) -> tuple[int, int]:
    """Validate and insert a batch with set-based SQL, under the caller's transaction."""
    db.execute("CREATE TEMP TABLE incoming AS SELECT * FROM events WHERE 0")
    db.executemany(
        "INSERT INTO incoming VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (
                event["event_id"],
                event["workflow_id"],
                event["event_type"],
                event["occurred_at"],
                epoch_microseconds(event["occurred_at"]),
                event["channel"],
                event["document_type"],
                event["failure_code"],
                event_hash(event),
                batch_id,
            )
            for event in events
        ),
    )
    conflict = db.execute(
        "SELECT 1 FROM incoming GROUP BY event_id HAVING COUNT(DISTINCT payload_sha256)>1 "
        "UNION ALL SELECT 1 FROM incoming i JOIN events e USING(event_id) "
        "WHERE i.payload_sha256 <> e.payload_sha256 LIMIT 1"
    ).fetchone()
    if conflict:
        raise PipelineError(
            "EVENT_ID_CONFLICT", "Existing event_id has a different canonical payload."
        )
    inserted = db.execute(
        "INSERT INTO events SELECT DISTINCT i.* FROM incoming i "
        "LEFT JOIN events e ON e.event_id=i.event_id WHERE e.event_id IS NULL"
    ).rowcount
    db.execute("CREATE TEMP TABLE affected_workflows AS SELECT DISTINCT workflow_id FROM incoming")
    violation = db.execute(sql_text("lifecycle_checks.sql")).fetchone()
    if violation:
        raise PipelineError(
            violation["error_code"], "Batch violates the workflow lifecycle contract."
        )
    return inserted, len(events) - inserted


def ingest(
    source: Path,
    root: Path,
    dataset_kind: str = "synthetic",
    sla_seconds: int = 3600,
    *,
    skip_accepted: bool = False,
) -> dict:
    body = read_bounded(source)
    with closing(connect(root, dataset_kind, sla_seconds)) as db:
        digest, key = preserve_raw(root, body)
        if skip_accepted:
            prior = db.execute(
                "SELECT batch_id FROM batches WHERE raw_sha256=? AND status='accepted' "
                "ORDER BY finished_at, batch_id LIMIT 1",
                (digest,),
            ).fetchone()
            if prior:
                return {
                    "batch_id": prior[0],
                    "status": "skipped",
                    "raw_sha256": digest,
                    "raw_key": key,
                    "input_rows": 0,
                    "inserted_rows": 0,
                    "duplicate_rows": 0,
                }
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
                inserted, duplicates = load_staging(db, events, batch_id)
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
