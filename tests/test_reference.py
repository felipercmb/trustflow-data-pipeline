import json
from pathlib import Path

from helpers import WarehouseTest

from trustflow.demo import generate
from trustflow.pipeline import ingest
from trustflow.report import report


class ReferenceTests(WarehouseTest):
    def test_published_synthetic_report_is_reproducible(self):
        source = self.path / "demo"
        generate(source, count=240, seed=42)
        for partition in sorted(source.glob("*.ndjson")):
            ingest(partition, self.root)
        result = report(self.root, "2025-01-16T00:00:00Z")
        reference = Path(__file__).resolve().parents[1] / "examples/demo-report/report.json"
        self.assertEqual(result, json.loads(reference.read_text(encoding="utf-8")))
