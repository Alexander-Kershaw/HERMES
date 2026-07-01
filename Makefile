.PHONY: install install-spark install-full lint test check clean

install:
	pip install -e ".[dev]"

install-spark:
	pip install -e ".[dev,spark]"

install-full:
	pip install -e ".[dev,spark,streaming,dbt]"

lint:
	ruff check .

format:
	ruff format .

test:
	pytest

check: lint test

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +
	find . -name ".DS_Store" -delete