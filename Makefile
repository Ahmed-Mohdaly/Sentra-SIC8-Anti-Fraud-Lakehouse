.PHONY: help check test contract-test demo-local lint docs
help:
	@echo 'make test | contract-test | demo-local | check | lint | docs'
test:
	python -m pytest -q
contract-test:
	python -m pytest -q tests/contracts
demo-local:
	PYTHONPATH=. python scripts/dev/demo_local.py --output build/demo
check: test demo-local
lint:
	python -m compileall -q src apps orchestration scripts
	@echo 'Python compileall completed. Ruff: run python -m ruff check . when installed.'
docs:
	@echo 'See README.md, README_AR.md, STATUS.md, docs/team/OWNERSHIP.md'
