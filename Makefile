# =============================================================================
# AVIV Data Platform — Case Study
# =============================================================================
#
# Usage:
#   make              → full pipeline: install → seed → run → test → insights
#   make install      → create .venv and install dbt-duckdb
#   make seed         → load CSV seeds into DuckDB
#   make run          → build all dbt models (staging + core + marts)
#   make test         → run all 52 dbt tests
#   make insights     → print business analysis queries to the terminal
#   make clean        → remove .venv, target/, dev.duckdb, logs/
#   make help         → show this message
#
# Requirements: Python 3.9–3.12  (3.13+ not yet supported by dbt)
# =============================================================================

VENV := .venv
DBT  := $(VENV)/bin/dbt
PY   := $(VENV)/bin/python3
OPTS := --profiles-dir . --no-partial-parse

# ---------------------------------------------------------------------------
# Auto-detect a dbt-compatible Python (3.9–3.12; 3.13+ not yet supported).
# Searches PATH in version-descending order; works with pyenv shims too.
# ---------------------------------------------------------------------------
PYTHON := $(shell \
	for cmd in python3.12 python3.11 python3.10 python3.9; do \
		if $$cmd -c "import sys" >/dev/null 2>&1; then echo $$cmd; break; fi; \
	done)

.DEFAULT_GOAL := all
.PHONY: all install seed run test insights clean help

# ---------------------------------------------------------------------------
# all — full pipeline (default target)
# ---------------------------------------------------------------------------
all: install seed run test insights

# ---------------------------------------------------------------------------
# install — create virtual environment and install dbt-duckdb
# ---------------------------------------------------------------------------
install:
	@echo ""
	@echo "============================================================"
	@echo "  Installing dependencies"
	@echo "============================================================"
	@if [ -z "$(PYTHON)" ]; then \
		echo ""; \
		echo "  ERROR: no compatible Python found (need 3.9–3.12)."; \
		echo "  Install one with:  brew install python@3.11"; \
		echo ""; \
		exit 1; \
	fi
	@echo "  Python  : $(PYTHON)"
	@echo "  Venv    : $(VENV)/"
	@echo ""
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip --quiet
	$(VENV)/bin/pip install "dbt-duckdb>=1.8,<2.0" --quiet
	@echo ""
	@echo "  ✓ .venv ready"

# ---------------------------------------------------------------------------
# seed — load CSV files into DuckDB via dbt seed
# ---------------------------------------------------------------------------
seed:
	@echo ""
	@echo "============================================================"
	@echo "  Loading seeds  (raw_listings.csv + raw_leads.csv)"
	@echo "============================================================"
	$(DBT) seed $(OPTS)

# ---------------------------------------------------------------------------
# run — compile and execute all dbt models
# ---------------------------------------------------------------------------
run:
	@echo ""
	@echo "============================================================"
	@echo "  Building models  (staging → core → marts)"
	@echo "============================================================"
	$(DBT) run $(OPTS)

# ---------------------------------------------------------------------------
# test — execute all dbt schema tests
# ---------------------------------------------------------------------------
test:
	@echo ""
	@echo "============================================================"
	@echo "  Running tests"
	@echo "============================================================"
	$(DBT) test $(OPTS)

# ---------------------------------------------------------------------------
# insights — run business analysis queries and print results
# ---------------------------------------------------------------------------
insights:
	@echo ""
	@echo "============================================================"
	@echo "  Business Insights"
	@echo "============================================================"
	$(PY) scripts/run_analyses.py

# ---------------------------------------------------------------------------
# clean — remove all generated artefacts
# ---------------------------------------------------------------------------
clean:
	@echo ""
	@echo "  Removing: .venv/  target/  dev.duckdb  logs/  .user.yml"
	rm -rf $(VENV) target/ dev.duckdb dev.duckdb.wal logs/ .user.yml
	@echo "  ✓ Done"

# ---------------------------------------------------------------------------
# help
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "  AVIV Data Platform — available targets:"
	@echo ""
	@echo "    make              Full pipeline (install → seed → run → test → insights)"
	@echo "    make install      Create .venv and install dbt-duckdb"
	@echo "    make seed         Load CSV seeds into DuckDB"
	@echo "    make run          Build all dbt models"
	@echo "    make test         Run all 52 dbt tests"
	@echo "    make insights     Print business analysis to terminal"
	@echo "    make clean        Remove .venv, target/, dev.duckdb, logs/"
	@echo "    make help         Show this message"
	@echo ""
