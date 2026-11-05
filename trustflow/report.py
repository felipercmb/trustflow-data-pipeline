"""SQL reports with explicit event-time cutoff and documented denominators."""

import csv
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

from .contract import CHANNELS, utc_timestamp
from .errors import PipelineError
from .storage import sql_text


def report(root: Path, as_of: str, channel: str | None = None) -> dict:
    as_of = utc_timestamp(as_of)
    if channel is not None and channel not in CHANNELS:
        raise PipelineError("INVALID_CHANNEL", "Unknown report channel.")
    warehouse = root / "warehouse.sqlite"
    if not warehouse.exists():
        raise PipelineError("NO_WAREHOUSE", "Ingest a batch before generating a report.")
    with closing(sqlite3.connect(warehouse.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        # A read transaction keeps all queries on the same snapshot during concurrent writes.
        db.execute("BEGIN")
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
        }
        cohorts = [dict(row) for row in db.execute(sql_text("cohort_metrics.sql"), params)]
        orphans = db.execute(
            "SELECT COUNT(*) FROM (SELECT workflow_id FROM events WHERE occurred_at <= :as_of "
            "AND (:channel IS NULL OR channel = :channel) GROUP BY workflow_id HAVING SUM(event_type = 'requested') = 0)",
            params,
        ).fetchone()[0]
        failure_codes = [
            dict(row)
            for row in db.execute(
                "SELECT failure_code, COUNT(*) AS event_count FROM events WHERE event_type='failed' "
                "AND occurred_at <= :as_of AND (:channel IS NULL OR channel=:channel) GROUP BY failure_code ORDER BY failure_code",
                params,
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
        return {
            "dataset_kind": settings["dataset_kind"],
            "as_of": as_of,
            "sla_seconds": settings["sla_seconds"],
            "channel": channel,
            "totals": totals,
            "cohorts": cohorts,
            "failure_events_by_code": failure_codes,
            "quality": {
                "workflows_awaiting_request": orphans,
                "batches_by_status_current": batches,
            },
            "semantics": "Event-time snapshot using all data currently ingested; late data can revise past reports.",
        }


def export_report(result: dict, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    with (output / "cohorts.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = (
            list(result["cohorts"][0])
            if result["cohorts"]
            else [
                "cohort_date",
                "requested_count",
                "signed_count",
                "failed_count",
                "canceled_count",
                "expired_count",
                "pending_count",
                "overdue_pending_count",
                "sla_due_count",
                "sla_met_count",
                "sla_met_pct",
                "avg_signed_seconds",
                "p95_signed_seconds",
            ]
        )
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(result["cohorts"])
