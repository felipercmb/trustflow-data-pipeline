PYTHON ?= python3

.PHONY: test demo report
test:
	$(PYTHON) -m unittest discover -s tests -v

demo:
	$(PYTHON) -m trustflow generate-demo --output data/demo
	$(PYTHON) -m trustflow ingest data/demo
	$(PYTHON) -m trustflow report --as-of 2025-01-16T00:00:00Z --output reports/demo

report:
	$(PYTHON) -m trustflow report --as-of 2025-01-16T00:00:00Z --output reports/demo
