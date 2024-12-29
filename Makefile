.PHONY: verify
verify:
	make .format_check test coverage

.PHONY: install
install:
	@make .poetry_install
	@poetry install --all-extras

.PHONY: format
format:
	poetry run black --safe .

.PHONY: lint
lint:
	make .ruff .format_check .poetry_check

.PHONY: test
test:
	poetry run pytest tests --asyncio-mode=auto --doctest-modules --junitxml=junit/test-results.xml tests/ --cov=data_collectors --cov-report=xml --cov-report=html

.PHONY: coverage
coverage:
	poetry run coverage report

.PHONY: build
build:
	@make .poetry_install
	@poetry build

.PHONY: poetry_install
.poetry_install:
	pip install poetry==1.6.1

.PHONY: poetry_check
.poetry_check:
	poetry check

.PHONY: format_check
.format_check:
	poetry run black --safe --check --diff --color .

.PHONY: ruff
.ruff:
	poetry run ruff check .
