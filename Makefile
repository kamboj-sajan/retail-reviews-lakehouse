# Run `make help` to list the targets.
TF      := terraform -chdir=infra/terraform
PY      := .venv/bin/python
SAMPLES := $(wildcard data/sample/*.jsonl.gz)

.PHONY: help venv test sample schema download-dry-run download tf-init tf-plan tf-apply destroy

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

venv: ## Create .venv and install the development tools
	python3 -m venv .venv
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -r requirements-dev.txt

test: ## Run the unit tests (no AWS access needed)
	$(PY) -m pytest -q

sample: ## Save the first lines of each source file into data/sample/
	python3 scripts/sample_raw.py

schema: ## Profile data/sample/ and print the observed schema as Markdown
	python3 scripts/inspect_schema.py --markdown --no-examples $(SAMPLES)

download-dry-run: ## List the source files and their sizes (no AWS access needed)
	python3 scripts/download_raw.py --dry-run

download: ## Copy the source files into the raw bucket (billable: S3 storage)
	$(PY) scripts/download_raw.py --bucket $$($(TF) output -raw raw_bucket)

tf-init: ## Download the Terraform providers
	$(TF) init

tf-plan: ## Show what Terraform would create
	$(TF) plan

tf-apply: ## Create the AWS resources (billable)
	$(TF) apply

destroy: ## TEARDOWN: delete every AWS resource in this project, including all data
	$(TF) destroy
