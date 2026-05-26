VENV := .venv
DBT  := $(VENV)/bin/dbt
OPTS := --profiles-dir . --no-partial-parse

# detect a dbt-compatible Python (3.9–3.12); pyenv shims supported
PYTHON := $(shell \
	for cmd in python3.12 python3.11 python3.10 python3.9; do \
		if $$cmd -c "import sys" >/dev/null 2>&1; then echo $$cmd; break; fi; \
	done)

.DEFAULT_GOAL := all
.PHONY: all install seed snapshot run test insights clean help

all: install seed snapshot run test insights

install:
	@if [ -z "$(PYTHON)" ]; then \
		echo "ERROR: no compatible Python found (need 3.9–3.12)."; \
		echo "  brew install python@3.11"; \
		exit 1; \
	fi
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip --quiet
	$(VENV)/bin/pip install "dbt-duckdb>=1.8,<2.0" --quiet
	$(DBT) deps $(OPTS)

seed:
	$(DBT) seed $(OPTS)

snapshot:
	$(DBT) run --select path:models/staging $(OPTS)
	$(DBT) snapshot $(OPTS)

run:
	$(DBT) run $(OPTS)

test:
	$(DBT) test $(OPTS)

insights:
	$(DBT) compile --select business_insights $(OPTS)
	duckdb dev.duckdb < target/compiled/aviv_data/analyses/business_insights.sql

clean:
	rm -rf $(VENV) target/ dev.duckdb dev.duckdb.wal logs/ .user.yml

help:
	@echo ""
	@echo "  make              install → seed → snapshot → run → test → insights"
	@echo "  make install      create .venv and install dbt-duckdb (Python 3.9–3.12)"
	@echo "  make seed         load CSV seeds"
	@echo "  make snapshot     SCD Type 2 snapshot + valid_from backfill"
	@echo "  make run          build all models"
	@echo "  make test         run all 68 tests"
	@echo "  make insights     compile and run business insight queries"
	@echo "  make clean        remove .venv, target/, dev.duckdb, logs/"
	@echo ""
