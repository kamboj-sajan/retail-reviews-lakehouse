# Run `make help` to list the targets.
TF      := terraform -chdir=infra/terraform
PY      := .venv/bin/python
SAMPLES := $(shell find data/sample/raw -name '*.jsonl.gz' 2>/dev/null | sort)
# The jobs import the `lakehouse` package from src/.
SPARK   := PYTHONPATH=src $(PY)
LOCAL   := data/local

# Spark 4 (Glue 6.0) needs Java 17. Use one registered with macOS if present,
# otherwise Homebrew's openjdk@17. An explicit JAVA_HOME always wins.
# -F matters: without it, java_home quietly returns another version (11).
JAVA_HOME ?= $(shell /usr/libexec/java_home -F -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home)
export JAVA_HOME

.PHONY: help venv test sample schema local-pipeline glue-run replay idempotency-test fault-injection-test reset-months download-dry-run download tf-init tf-plan tf-apply destroy

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

venv: ## Create .venv (Python 3.13, same as Glue 6.0) and install the tools; needs uv
	uv venv --python 3.13 .venv
	uv pip install --python $(PY) -r requirements-dev.txt

test: ## Run all tests (no AWS access needed; uses data/sample if present)
	$(PY) -m pytest -q

sample: ## Save the first lines of each source file into data/sample/raw/
	python3 scripts/sample_raw.py

schema: ## Profile data/sample/raw and print the observed schema as Markdown
	python3 scripts/inspect_schema.py --markdown --no-examples $(SAMPLES)

local-pipeline: ## Run every job on the local sample, writing to data/local/
	$(SPARK) jobs/clean_reviews.py  --raw_path data/sample/raw --cleaned_path $(LOCAL)/cleaned --quarantine_path $(LOCAL)/quarantine
	$(SPARK) jobs/clean_products.py --raw_path data/sample/raw --cleaned_path $(LOCAL)/cleaned --quarantine_path $(LOCAL)/quarantine
	$(SPARK) jobs/build_curated.py  --cleaned_path $(LOCAL)/cleaned --curated_path $(LOCAL)/curated

glue-run: ## Run a Glue job and report its cost (billable), e.g. make glue-run JOB=rrl-clean-reviews ARGS="--arg categories=Video_Games"
	$(PY) scripts/run_glue_job.py $(JOB) $(ARGS)

BUCKET = $$($(TF) output -json buckets | $(PY) -c "import json,sys; print(json.load(sys.stdin)['$(1)'])")
STATE_MACHINE = $$($(TF) output -raw monthly_load_state_machine)

replay: ## Deliver months and wait for the loads they trigger, e.g. make replay MONTHS="2022-10 2022-11"
	$(PY) scripts/replay_month.py $(MONTHS) --bucket $(call BUCKET,raw) --state-machine $(STATE_MACHINE) --wait

idempotency-test: ## Load one month twice through Step Functions and compare counts, e.g. MONTH=2023-01
	$(PY) scripts/test_month_idempotency.py $(MONTH) --state-machine $(STATE_MACHINE) --cleaned $(call BUCKET,cleaned) --curated $(call BUCKET,curated)

fault-injection-test: ## Inject bad rows into a month, prove quarantine and the quality gate work, clean up (MONTH=2023-09)
	$(PY) scripts/fault_injection_test.py --raw $(call BUCKET,raw) --artifacts $(call BUCKET,artifacts) --state-machine $(STATE_MACHINE) --month $(or $(MONTH),2023-09)

reset-months: ## List (or with YES=1 delete) months FROM..TO from cleaned, curated and landing
	$(PY) scripts/reset_months.py --raw $(call BUCKET,raw) --cleaned $(call BUCKET,cleaned) --curated $(call BUCKET,curated) --from $(FROM) --to $(TO) $(if $(YES),--yes)

download-dry-run: ## List the source files and their sizes (no AWS access needed)
	python3 scripts/download_raw.py --dry-run

download: ## Copy the source files into the raw bucket (billable: S3 storage)
	$(PY) scripts/download_raw.py --bucket $$($(TF) output -raw raw_bucket) --region $$($(TF) output -raw aws_region)

tf-init: ## Download the Terraform providers
	$(TF) init

tf-plan: ## Show what Terraform would create
	$(TF) plan

tf-apply: ## Plan, refuse any delete/replace unless ALLOW_DESTROY=1, then apply that exact plan
	$(TF) plan -input=false -out=tfplan
	$(TF) show -json tfplan | $(PY) scripts/check_plan.py
	$(TF) apply -input=false tfplan
	rm -f infra/terraform/tfplan

destroy: ## TEARDOWN: delete every AWS resource in this project, including all data
	$(TF) destroy
