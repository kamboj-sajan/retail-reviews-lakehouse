# Run `make help` to list the targets.
TF      := terraform -chdir=infra/terraform
PY      := .venv/bin/python
SAMPLES := $(shell find data/sample/raw -name '*.jsonl.gz' 2>/dev/null | sort)
# The jobs import the `lakehouse` package from src/.
SPARK   := PYTHONPATH=src $(PY)
LOCAL   := data/local

.PHONY: help venv test sample schema local-pipeline download-dry-run download tf-init tf-plan tf-apply destroy

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

venv: ## Create .venv (Python 3.11, same as Glue 5.1) and install the tools; needs uv
	uv venv --python 3.11 .venv
	uv pip install --python $(PY) -r requirements-dev.txt

test: ## Run all tests (no AWS access needed; uses data/sample if present)
	$(PY) -m pytest -q

sample: ## Save the first lines of each source file into data/sample/raw/
	python3 scripts/sample_raw.py

schema: ## Profile data/sample/raw and print the observed schema as Markdown
	python3 scripts/inspect_schema.py --markdown --no-examples $(SAMPLES)

local-pipeline: ## Run every job on the local sample, writing to data/local/
	$(SPARK) jobs/clean_reviews.py  --raw_path data/sample/raw --cleaned_path $(LOCAL)/cleaned
	$(SPARK) jobs/clean_products.py --raw_path data/sample/raw --cleaned_path $(LOCAL)/cleaned
	$(SPARK) jobs/build_curated.py  --cleaned_path $(LOCAL)/cleaned --curated_path $(LOCAL)/curated

download-dry-run: ## List the source files and their sizes (no AWS access needed)
	python3 scripts/download_raw.py --dry-run

download: ## Copy the source files into the raw bucket (billable: S3 storage)
	$(PY) scripts/download_raw.py --bucket $$($(TF) output -raw raw_bucket) --region $$($(TF) output -raw aws_region)

tf-init: ## Download the Terraform providers
	$(TF) init

tf-plan: ## Show what Terraform would create
	$(TF) plan

tf-apply: ## Create the AWS resources (billable)
	$(TF) apply

destroy: ## TEARDOWN: delete every AWS resource in this project, including all data
	$(TF) destroy
