"""Rebuild accepted facts into a new warehouse; never overwrite the source."""

import os
import shutil
import tempfile
from contextlib import closing
from pathlib import Path

from .audit import audit_snapshot, fact_fingerprint, verified_raw
from .errors import PipelineError
from .pipeline import ingest
from .storage import readonly


def rebuild(source: Path, destination: Path) -> dict:
    source, destination = source.resolve(), destination.resolve()
    if destination.exists() or destination.is_relative_to(source):
        raise PipelineError(
            "UNSAFE_DESTINATION", "Rebuild requires a new destination outside the source warehouse."
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with closing(readonly(source)) as db:
        # Hold the source snapshot for the entire rebuild. This may delay source writers.
        verification = audit_snapshot(db, source)
        if verification["status"] != "passed" or verification["running_batches"]:
            raise PipelineError(
                "SOURCE_AUDIT_FAILED",
                "Resolve integrity findings and running batches before rebuilding.",
            )
        config = dict(db.execute("SELECT * FROM settings").fetchone())
        batches = db.execute(
            "SELECT * FROM batches WHERE status='accepted' ORDER BY finished_at, batch_id"
        ).fetchall()
        if not batches:
            raise PipelineError("NO_ACCEPTED_BATCHES", "There are no accepted batches to rebuild.")
        staging = Path(tempfile.mkdtemp(prefix=".trustflow-rebuild-", dir=destination.parent))
        try:
            seen = set()
            for batch in batches:
                if batch["raw_sha256"] in seen:
                    continue
                body = verified_raw(source, batch)
                replay = staging / "replay.ndjson"
                replay.write_bytes(body)
                ingest(replay, staging, config["dataset_kind"], config["sla_seconds"])
                replay.unlink()
                seen.add(batch["raw_sha256"])
            with closing(readonly(staging)) as rebuilt:
                fingerprint = fact_fingerprint(rebuilt)
                if fingerprint != fact_fingerprint(db):
                    raise PipelineError(
                        "REBUILD_MISMATCH", "Rebuilt facts do not match the source snapshot."
                    )
            if destination.exists():
                raise PipelineError(
                    "UNSAFE_DESTINATION",
                    "Destination appeared during rebuild; refusing replacement.",
                )
            os.rename(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging)  # Only the temporary directory created above.
        return {
            "status": "rebuilt",
            "dataset_kind": config["dataset_kind"],
            "replayed_raw_files": len(seen),
            "events": verification["checked_events"],
            "fact_sha256": fingerprint,
            "knowledge_history": "new ingestion timestamps; use a full backup to preserve the original history",
        }
