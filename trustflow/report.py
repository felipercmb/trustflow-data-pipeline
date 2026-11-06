"""SQL reports with explicit event-time cutoff and documented denominators."""

import hashlib
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

from .contract import CHANNELS, DOCUMENT_TYPES, epoch_microseconds, utc_timestamp
from .errors import PipelineError
from .export import export_report as export_report
from .storage import readonly, sql_text


def report(
    root: Path,
    as_of: str,
    channel: str | None = None,
    *,
    known_at: str | None = None,
    document_type: str | None = None,
) -> dict:
    as_of = utc_timestamp(as_of)
    if channel is not None and channel not in CHANNELS:
        raise PipelineError("INVALID_CHANNEL", "Unknown report channel.")
    if document_type is not None and document_type not in DOCUMENT_TYPES:
        raise PipelineError("INVALID_DOCUMENT_TYPE", "Unknown report document type.")
    known_at = utc_timestamp(known_at) if known_at is not None else None
    with closing(readonly(root)) as db:
        settings = dict(db.execute("SELECT * FROM settings").fetchone())
        try:
            cutoff = (
                datetime.fromisoformat(as_of) - timedelta(seconds=settings["sla_seconds"])
            ).isoformat(timespec="microseconds")
        except OverflowError as exc:
            raise PipelineError(
                "INVALID_TIMESTAMP", "Observation date cannot accommodate this SLA."
            ) from exc
        params = {
            "as_of": as_of,
            "sla_cutoff": cutoff,
            "sla_seconds": settings["sla_seconds"],
            "channel": channel,
            "document_type": document_type,
            "known_at": known_at,
            "as_of_us": epoch_microseconds(as_of),
        }
        db.execute(
            "CREATE TEMP TABLE snapshot_events AS SELECT e.* FROM events e "
            "JOIN batches b ON b.batch_id=e.source_batch_id WHERE b.status='accepted' "
            "AND e.occurred_at <= :as_of AND (:known_at IS NULL OR b.finished_at <= :known_at) "
            "AND (:channel IS NULL OR e.channel=:channel) "
            "AND (:document_type IS NULL OR e.document_type=:document_type)",
            params,
        )
        db.execute("CREATE TEMP TABLE snapshot_workflows AS " + sql_text("snapshot_lifecycle.sql"))
        cohorts = [dict(row) for row in db.execute(sql_text("cohort_metrics.sql"), params)]
        orphans = db.execute(
            "SELECT COUNT(*) FROM snapshot_workflows WHERE requested_at IS NULL"
        ).fetchone()[0]
        failure_codes = [
            dict(row)
            for row in db.execute(
                "SELECT failure_code, COUNT(*) AS event_count FROM snapshot_events WHERE event_type='failed' "
                "GROUP BY failure_code ORDER BY failure_code",
            )
        ]
        batches = {
            row["status"]: row["n"]
            for row in db.execute("SELECT status, COUNT(*) AS n FROM batches GROUP BY status")
        }
        totals = {
            field: sum(row[field] for row in cohorts)
            for field in (
                "requested_count",
                "signed_count",
                "failed_count",
                "canceled_count",
                "expired_count",
                "pending_count",
                "overdue_pending_count",
                "sla_due_count",
                "sla_met_count",
            )
        }
        totals["sla_met_pct"] = (
            round(100 * totals["sla_met_count"] / totals["sla_due_count"], 2)
            if totals["sla_due_count"]
            else None
        )
        segments = [dict(row) for row in db.execute(sql_text("segment_metrics.sql"), params)]
        backlog = [dict(row) for row in db.execute(sql_text("backlog_metrics.sql"), params)]
        fingerprint = hashlib.sha256()
        source_events = 0
        for row in db.execute(
            "SELECT event_id, payload_sha256 FROM snapshot_events ORDER BY event_id"
        ):
            fingerprint.update(f"{row[0]}:{row[1]}\n".encode("ascii"))
            source_events += 1
        return {
            "report_version": 2,
            "dataset_kind": settings["dataset_kind"],
            "as_of": as_of,
            "known_at": known_at,
            "document_type": document_type,
            "source_event_count": source_events,
            "event_snapshot_sha256": fingerprint.hexdigest(),
            "sla_seconds": settings["sla_seconds"],
            "channel": channel,
            "totals": totals,
            "cohorts": cohorts,
            "segments": segments,
            "backlog": backlog,
            "failure_events_by_code": failure_codes,
            "quality": {
                "workflows_awaiting_request": orphans,
                "batches_by_status_current": batches,
            },
            "semantics": "Event-time snapshot; known_at optionally limits first accepted ingestion time. Audit counters reflect current warehouse state.",
        }
