"""Publish complete immutable report bundles with a hash manifest."""

import csv
import hashlib
import io
import json
import os
import shutil
import tempfile
from pathlib import Path

from .errors import PipelineError

CSV_FIELDS = {
    "cohorts": [
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
    ],
    "segments": [
        "channel",
        "document_type",
        "requested_count",
        "signed_count",
        "failed_count",
        "canceled_count",
        "expired_count",
        "pending_count",
        "overdue_pending_count",
        "sla_due_count",
        "sla_met_count",
        "avg_signed_seconds",
    ],
    "backlog": ["bucket", "pending_count", "oldest_age_seconds"],
}


def verify_export(output: Path) -> dict:
    try:
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        expected_names = {"report.json", "cohorts.csv", "segments.csv", "backlog.csv"}
        if (
            not isinstance(manifest, dict)
            or manifest.get("manifest_version") != 1
            or not isinstance(manifest.get("files"), dict)
            or set(manifest["files"]) != expected_names
        ):
            raise ValueError
        failures = []
        for name, expected in manifest["files"].items():
            path = output / name
            if (
                not path.is_file()
                or path.is_symlink()
                or hashlib.sha256(path.read_bytes()).hexdigest() != expected
            ):
                failures.append(name)
        if not failures:
            result = json.loads((output / "report.json").read_text(encoding="utf-8"))
            if (
                result["dataset_kind"] != manifest["dataset_kind"]
                or result["event_snapshot_sha256"] != manifest["event_snapshot_sha256"]
            ):
                failures.append("manifest_metadata")
        return {
            "status": "failed" if failures else "passed",
            "verified_files": len(expected_names)
            - len([name for name in failures if name in expected_names]),
            "failures": failures,
        }
    except (ValueError, KeyError, TypeError, OSError) as exc:
        raise PipelineError("INVALID_MANIFEST", "Report manifest is missing or invalid.") from exc


def export_report(result: dict, output: Path) -> None:
    payloads = {
        "report.json": (json.dumps(result, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    }
    for key, fields in CSV_FIELDS.items():
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(result[key])
        payloads[f"{key}.csv"] = stream.getvalue().encode("utf-8")
    manifest = {
        "manifest_version": 1,
        "dataset_kind": result["dataset_kind"],
        "event_snapshot_sha256": result["event_snapshot_sha256"],
        "files": {name: hashlib.sha256(body).hexdigest() for name, body in payloads.items()},
    }
    payloads["manifest.json"] = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    if output.exists():
        if (
            output.is_dir()
            and {p.name for p in output.iterdir()} == set(payloads)
            and all(
                (output / name).is_file() and (output / name).read_bytes() == body
                for name, body in payloads.items()
            )
        ):
            return
        raise PipelineError(
            "OUTPUT_EXISTS", "Choose a new output directory; existing report bundles are immutable."
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".trustflow-export-", dir=output.parent))
    try:
        for name, body in payloads.items():
            with (staging / name).open("wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        if output.exists():
            raise PipelineError("OUTPUT_EXISTS", "Report destination appeared during export.")
        os.rename(staging, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
