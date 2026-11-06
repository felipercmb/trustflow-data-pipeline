"""Read-only verification of raw integrity, accounting and event provenance."""

import hashlib
import re
import sqlite3
from contextlib import closing
from pathlib import Path

from .contract import epoch_microseconds, event_hash
from .errors import PipelineError
from .pipeline import parse_batch
from .storage import read_bounded, readonly, sql_text


def verified_raw(root: Path, batch: dict | sqlite3.Row) -> bytes:
    digest = batch["raw_sha256"]
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise PipelineError("RAW_INTEGRITY", "Invalid raw content address.")
    expected_key = f"bronze/{digest[:2]}/{digest}.ndjson"
    if batch["raw_key"] != expected_key:
        raise PipelineError("RAW_INTEGRITY", "Raw key does not match its content address.")
    path = root / expected_key
    if not path.resolve().is_relative_to(root.resolve()):
        raise PipelineError("RAW_INTEGRITY", "Raw file resolves outside the warehouse.")
    try:
        body = read_bounded(path)
    except (OSError, PipelineError) as exc:
        raise PipelineError(
            "RAW_INTEGRITY", "Raw file is missing, inaccessible or oversized."
        ) from exc
    if hashlib.sha256(body).hexdigest() != digest:
        raise PipelineError("RAW_INTEGRITY", "Raw file failed SHA-256 verification.")
    return body


def audit_snapshot(db: sqlite3.Connection, root: Path) -> dict:
    """Inspect a stable database snapshot, including raw files for rejected attempts."""
    findings = []
    config = dict(db.execute("SELECT * FROM settings").fetchone())
    integrity = [row[0] for row in db.execute("PRAGMA quick_check")]
    if integrity != ["ok"]:
        findings.append({"code": "DATABASE_INTEGRITY", "count": len(integrity)})
    foreign = db.execute("PRAGMA foreign_key_check").fetchall()
    if foreign:
        findings.append({"code": "FOREIGN_KEY", "count": len(foreign)})
    batches = db.execute("SELECT * FROM batches ORDER BY started_at, batch_id").fetchall()
    checked = set()
    for batch in batches:
        key = batch["raw_sha256"]
        try:
            # Validate the address of EVERY batch, even when content is repeated.
            body = verified_raw(root, batch)
            checked.add(key)
            if batch["status"] != "accepted":
                if batch["inserted_rows"] or batch["duplicate_rows"]:
                    findings.append({"code": "BATCH_ACCOUNTING", "batch_id": batch["batch_id"]})
                continue
            events = parse_batch(body, config["dataset_kind"])
            if (
                len(events) != batch["input_rows"]
                or (batch["inserted_rows"] + batch["duplicate_rows"] != batch["input_rows"])
                or batch["finished_at"] is None
            ):
                findings.append({"code": "BATCH_ACCOUNTING", "batch_id": batch["batch_id"]})
            expected = {event["event_id"]: event for event in events}
            if any(
                event_hash(event) != event_hash(expected[event["event_id"]]) for event in events
            ):
                findings.append({"code": "EVENT_ID_CONFLICT", "batch_id": batch["batch_id"]})
            # The temporary table makes this a bulk join rather than one lookup per event.
            db.execute("DROP TABLE IF EXISTS temp.audit_expected")
            db.execute(
                "CREATE TEMP TABLE audit_expected(event_id TEXT PRIMARY KEY, payload_sha256 TEXT)"
            )
            db.executemany(
                "INSERT INTO audit_expected VALUES (?, ?)",
                ((key, event_hash(value)) for key, value in expected.items()),
            )
            missing = db.execute(
                "SELECT COUNT(*) FROM audit_expected a LEFT JOIN events e USING(event_id) "
                "WHERE e.event_id IS NULL OR a.payload_sha256<>e.payload_sha256"
            ).fetchone()[0]
            if missing:
                findings.append(
                    {"code": "RAW_EVENT_MISMATCH", "batch_id": batch["batch_id"], "count": missing}
                )
            owned = db.execute(
                "SELECT * FROM events WHERE source_batch_id=?", (batch["batch_id"],)
            ).fetchall()
            if len(owned) != batch["inserted_rows"]:
                findings.append({"code": "BATCH_ACCOUNTING", "batch_id": batch["batch_id"]})
            for row in owned:
                original = expected.get(row["event_id"])
                if (
                    original is None
                    or any(
                        row[field] != original[field]
                        for field in (
                            "event_id",
                            "workflow_id",
                            "event_type",
                            "occurred_at",
                            "channel",
                            "document_type",
                            "failure_code",
                        )
                    )
                    or row["occurred_at_us"] != epoch_microseconds(original["occurred_at"])
                ):
                    findings.append({"code": "EVENT_LINEAGE", "batch_id": batch["batch_id"]})
                    break
        except PipelineError as exc:
            findings.append({"code": exc.code, "batch_id": batch["batch_id"]})
    invalid_owners = db.execute(
        "SELECT COUNT(*) FROM events e LEFT JOIN batches b ON b.batch_id=e.source_batch_id "
        "WHERE b.batch_id IS NULL OR b.status<>'accepted'"
    ).fetchone()[0]
    if invalid_owners:
        findings.append({"code": "INVALID_EVENT_OWNER", "count": invalid_owners})
    db.execute("CREATE TEMP TABLE affected_workflows AS SELECT DISTINCT workflow_id FROM events")
    violation = db.execute(sql_text("lifecycle_checks.sql")).fetchone()
    if violation:
        findings.append({"code": violation["error_code"]})
    running = sum(row["status"] == "running" for row in batches)
    return {
        "status": "failed" if findings else "passed",
        "dataset_kind": config["dataset_kind"],
        "checked_raw_files": len(checked),
        "checked_batches": len(batches),
        "checked_events": db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
        "running_batches": running,
        "findings": findings,
    }


def audit(root: Path) -> dict:
    with closing(readonly(root)) as db:
        return audit_snapshot(db, root)


def fact_fingerprint(db: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    for row in db.execute("SELECT event_id, payload_sha256 FROM events ORDER BY event_id"):
        digest.update(f"{row[0]}:{row[1]}\n".encode("ascii"))
    return digest.hexdigest()
