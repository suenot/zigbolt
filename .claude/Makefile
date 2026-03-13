.PHONY: dashboard scan map activity status init

# Auto-detected Python with cmdop_claude installed
PYTHON := /opt/homebrew/opt/python@3.11/bin/python3.11

# ── cmdop-claude convenience commands ────────────────────────────────

dashboard:
	$(PYTHON) -m streamlit run $$($(PYTHON) -c "from pathlib import Path; import cmdop_claude.ui; print(Path(cmdop_claude.ui.__file__).parent / 'main.py')")

scan:
	$(PYTHON) -m cmdop_claude.sidecar.hook review

map:
	$(PYTHON) -m cmdop_claude.sidecar.hook map-update

activity:
	$(PYTHON) -m cmdop_claude.sidecar.hook activity

status:
	$(PYTHON) -m cmdop_claude.sidecar.hook status

init:
	$(PYTHON) -m cmdop_claude.sidecar.hook init
