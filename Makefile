PYTHON ?= .venv/bin/python
MVN ?= mvn
COMPOSE = docker compose -f deployment/compose.yaml

.PHONY: setup test java-test verify demo up down generate replay smoke drill lint query-check query-benchmark reconcile
setup:
	UV_CACHE_DIR=.cache/uv uv venv --python 3.12
	UV_CACHE_DIR=.cache/uv uv pip sync --python .venv/bin/python requirements.lock
	UV_CACHE_DIR=.cache/uv uv pip install --python .venv/bin/python --no-deps -e .
test:
	$(PYTHON) -m pytest -q
java-test:
	$(MVN) -f flink-jobs/pom.xml -Dmaven.repo.local=$(CURDIR)/.cache/m2 verify
lint:
	.venv/bin/ruff check adpulse tests scripts
verify: lint test java-test
demo:
	$(PYTHON) -m adpulse.cli demo --output artifacts/demo
up: java-test
	$(COMPOSE) up -d --build --wait
down:
	$(COMPOSE) down
generate:
	$(PYTHON) -m adpulse.cli generate --output artifacts/input --send http://localhost:8088 --users 1000
smoke:
	$(PYTHON) scripts/integration.py
drill:
	$(PYTHON) scripts/drills.py --scenario $(SCENARIO)
query-check:
	$(PYTHON) scripts/query_acceptance.py --release $(RELEASE) --output artifacts/query-acceptance.json
query-benchmark:
	$(PYTHON) scripts/query_benchmark.py --release $(RELEASE) --output artifacts/query-benchmark.json
reconcile:
	$(PYTHON) scripts/disk_reconcile.py --output $(OUTPUT)
