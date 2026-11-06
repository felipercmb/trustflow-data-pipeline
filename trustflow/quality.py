"""Explicit quality gates; demo thresholds never imply a company's real SLA."""

import math
import tomllib
from pathlib import Path

from .errors import PipelineError

RULES = {
    "min_requested_count": ("gte", "event_snapshot"),
    "max_orphan_workflows": ("lte", "event_snapshot"),
    "max_rejected_batches": ("lte", "current_ingestion_audit"),
    "max_running_batches": ("lte", "current_ingestion_audit"),
    "min_sla_due_count": ("gte", "event_snapshot"),
    "min_sla_met_pct": ("gte", "event_snapshot"),
    "max_overdue_pending_count": ("lte", "event_snapshot"),
    "max_oldest_pending_seconds": ("lte", "event_snapshot"),
}


def load_policy(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            policy = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise PipelineError("INVALID_POLICY", "Policy is not valid TOML.") from exc
    if (
        set(policy) != {"policy", "rules"}
        or not isinstance(policy["policy"], dict)
        or not isinstance(policy["rules"], dict)
    ):
        raise PipelineError("INVALID_POLICY", "Policy requires exactly policy and rules tables.")
    metadata = policy["policy"]
    if (
        set(metadata) != {"version", "dataset_kind"}
        or type(metadata["version"]) is not int
        or metadata["version"] != 1
        or metadata["dataset_kind"] not in ("synthetic", "operational")
    ):
        raise PipelineError("INVALID_POLICY", "Invalid policy version or dataset kind.")
    rules = policy["rules"]
    if not rules or set(rules) - set(RULES):
        raise PipelineError(
            "INVALID_POLICY", "At least one known rule is required; unknown rules are rejected."
        )
    for name, value in rules.items():
        if name == "min_sla_met_pct":
            valid = type(value) in (int, float) and 0 <= value <= 100 and math.isfinite(value)
        else:
            valid = type(value) is int and 0 <= value <= 2**63 - 1
        if not valid:
            raise PipelineError(
                "INVALID_POLICY",
                "Thresholds must be nonnegative integers; SLA percentage must be between 0 and 100.",
            )
    return policy


def evaluate(result: dict, policy: dict) -> dict:
    if policy["policy"]["dataset_kind"] != result["dataset_kind"]:
        raise PipelineError(
            "POLICY_DATASET_MISMATCH", "Policy dataset kind does not match the report."
        )
    totals, quality = result["totals"], result["quality"]
    values = {
        "min_requested_count": totals["requested_count"],
        "max_orphan_workflows": quality["workflows_awaiting_request"],
        "max_rejected_batches": quality["batches_by_status_current"].get("rejected", 0),
        "max_running_batches": quality["batches_by_status_current"].get("running", 0),
        "min_sla_due_count": totals["sla_due_count"],
        "min_sla_met_pct": totals["sla_met_pct"],
        "max_overdue_pending_count": totals["overdue_pending_count"],
        "max_oldest_pending_seconds": max(
            (row["oldest_age_seconds"] for row in result["backlog"]), default=0
        ),
    }
    checks = []
    for name, threshold in policy["rules"].items():
        comparison, scope = RULES[name]
        actual = values[name]
        passed = actual is not None and (
            actual >= threshold if comparison == "gte" else actual <= threshold
        )
        checks.append(
            {
                "rule": name,
                "scope": scope,
                "actual": actual,
                "threshold": threshold,
                "comparison": comparison,
                "status": "passed" if passed else "failed",
                "reason": "no_eligible_population" if actual is None else None,
            }
        )
    return {
        "status": "passed" if all(row["status"] == "passed" for row in checks) else "failed",
        "dataset_kind": result["dataset_kind"],
        "as_of": result["as_of"],
        "known_at": result["known_at"],
        "event_snapshot_sha256": result["event_snapshot_sha256"],
        "checks": checks,
    }
