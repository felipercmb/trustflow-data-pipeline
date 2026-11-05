"""Reproducible SYNTHETIC scenarios; no client records or production estimates."""

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from .errors import PipelineError


def generate(output: Path, count: int = 240, seed: int = 42) -> dict:
    if not 1 <= count <= 10_000:
        raise PipelineError("INVALID_COUNT", "Demo count must be between 1 and 10,000 workflows.")
    if output.exists() and any(output.iterdir()):
        raise PipelineError(
            "OUTPUT_NOT_EMPTY",
            "Choose an empty demo output directory to preserve earlier data.",
        )
    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    partitions: dict[str, list[dict]] = {}
    event_count = 0
    for index in range(count):
        workflow_id = str(uuid5(NAMESPACE_URL, f"trustflow-synthetic:{seed}:{index}"))
        requested = start + timedelta(seconds=rng.randrange(14 * 86400))
        common = {
            "schema_version": 1,
            "workflow_id": workflow_id,
            "dataset_kind": "synthetic",
            "channel": rng.choice(["web", "mobile", "api"]),
            "document_type": rng.choice(["contract", "authorization", "service_order"]),
        }
        status = rng.choices(
            ["signed", "failed", "canceled", "expired", "pending"], [55, 15, 5, 5, 20]
        )[0]
        events = [("requested", requested)]
        if status != "pending":
            events.append((status, requested + timedelta(seconds=rng.randint(30, 10800))))
        for event_type, occurred in events:
            event = {
                **common,
                "event_id": str(uuid5(NAMESPACE_URL, f"{workflow_id}:{event_type}")),
                "event_type": event_type,
                "occurred_at": occurred.isoformat(),
                "failure_code": rng.choice(["PROVIDER_UNAVAILABLE", "INVALID_DOCUMENT", "TIMEOUT"])
                if event_type == "failed"
                else None,
            }
            partitions.setdefault(occurred.date().isoformat(), []).append(event)
            event_count += 1
    output.mkdir(parents=True, exist_ok=True)
    for day, events in sorted(partitions.items()):
        rng.shuffle(events)  # Physical order intentionally differs from event time.
        (output / f"{day}.ndjson").write_text(
            "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
            encoding="utf-8",
        )
    return {
        "dataset_kind": "synthetic",
        "seed": seed,
        "workflows": count,
        "events": event_count,
        "partitions": len(partitions),
        "suggested_as_of": "2025-01-16T00:00:00Z",
    }
