# Retail Reviews Lakehouse

A data lake and warehouse on AWS built on the Amazon Reviews 2023 dataset:
S3 for storage, PySpark on AWS Glue for ETL, Step Functions for monthly
incremental loads, Redshift Serverless for the warehouse, and Athena for
ad-hoc queries.

This project is being built in phases. The README describes only what exists.

## Status

| Phase | What | State |
|---|---|---|
| 0 | Repo, Terraform skeleton, budget alarm, S3 buckets, download script | Done: buckets and budget alarm created, source files copied into the raw bucket |
| 1 | Data lake layers: raw, cleaned, curated | Done: layout defined, jobs tested locally |
| 2 | PySpark ETL on Glue, Glue Data Catalog | Done: all three categories loaded through Glue; every table queryable in Athena |
| 3 | Incremental monthly loads with Step Functions and EventBridge | Done: 12 months replayed through EventBridge and Step Functions; re-running a month changes nothing |
| 4 | Redshift Serverless star schema | Not started |
| 5 | Data quality checks and quarantine | Done: bad rows quarantined with a reason; a quality gate fails any month that does not reconcile; proven by injecting bad rows on AWS |
| 6 | Dashboard | Not started |
| 7 | Benchmarks and results | Not started |

## Dataset

[Amazon Reviews 2023](https://amazon-reviews-2023.github.io/), collected by the
McAuley Lab at UC San Diego. It is provided for research use. This repository
contains no dataset files: the scripts download them from the lab's server into
your own S3 bucket, and `data/` is git-ignored.

Three categories are used: `Electronics`, `Toys_and_Games` and `Video_Games`.
The files and their sizes, as reported by the lab's server:

```
$ python3 scripts/download_raw.py --dry-run
kind     category                    bytes      GB  s3 key
reviews  Electronics            6474438619    6.47  reviews/category=Electronics/Electronics.jsonl.gz
meta     Electronics            1312900427    1.31  meta/category=Electronics/meta_Electronics.jsonl.gz
reviews  Toys_and_Games         1948910944    1.95  reviews/category=Toys_and_Games/Toys_and_Games.jsonl.gz
meta     Toys_and_Games          658655829    0.66  meta/category=Toys_and_Games/meta_Toys_and_Games.jsonl.gz
reviews  Video_Games             814206092    0.81  reviews/category=Video_Games/Video_Games.jsonl.gz
meta     Video_Games             103096082    0.10  meta/category=Video_Games/meta_Video_Games.jsonl.gz
total                          11312207993   11.31
```

(Run on 2026-11-10. GB here means 10^9 bytes.)

The dataset page lists 43.9M, 16.3M and 4.6M ratings for these categories.
This project's own counts are in [Results](#results).

### Citation

```bibtex
@article{hou2024bridging,
  title={Bridging Language and Items for Retrieval and Recommendation},
  author={Hou, Yupeng and Li, Jiacheng and He, Zhankui and Yan, An and Chen, Xiusi and McAuley, Julian},
  journal={arXiv preprint arXiv:2403.03952},
  year={2024}
}
```

## Repository layout

```
config/categories.json             which categories to load
infra/terraform/                   all AWS infrastructure
src/lakehouse/                     PySpark code shared by every job
jobs/                              Glue job entry points (also run locally)
sql/analysis/                      Athena queries behind design decisions
sql/checks/                        Athena queries that verify loaded data
scripts/download_raw.py            copies the source files into the raw S3 bucket
scripts/run_glue_job.py            runs a Glue job; records its time, DPU-seconds and cost
scripts/run_athena_query.py        runs a SQL file in Athena; records bytes scanned, time and cost
scripts/replay_month.py            delivers months to the landing area, which triggers their loads
scripts/reset_months.py            removes a range of months before a replay (dry run by default)
scripts/test_month_idempotency.py  loads one month twice through Step Functions and compares
scripts/fault_injection_test.py    injects bad rows into a month to prove quarantine and the quality gate
scripts/glue_run_costs.py          reads Glue runs back from Glue with their DPU-seconds and cost
scripts/check_plan.py              refuses a Terraform plan that deletes or replaces anything
scripts/sample_raw.py              saves a small local sample for development
scripts/inspect_schema.py          profiles the real fields in the sample
scripts/compare_parquet_codecs.py  measures Parquet size per compression codec
tests/                             tests (no AWS access needed)
benchmarks/                        one JSON line per Glue run and Athena query actually executed
docs/schema.md                     the schema observed in the real files
docs/data-lake-layout.md           paths, columns and types of every layer, and the quarantine rules
docs/decisions.md                  design decisions and trade-offs
```

## Running it

Requirements: [uv](https://docs.astral.sh/uv/) (it installs Python 3.13, the
version inside AWS Glue 6.0), Java 17 for Spark 4, Terraform 1.6+, and AWS
credentials in your environment (for example from `aws configure`). No
credentials are stored in this repository.

Locally, with no AWS access:

```
make venv                 # create .venv: Python 3.13, PySpark 4.1.1, boto3, pytest, moto
make sample               # save the first 20,000 reviews and 5,000 products per category
make test                 # all tests, including an end-to-end run on the sample
make local-pipeline       # run every job on the sample, writing to data/local/
make download-dry-run     # list source files and sizes
```

On AWS:

```
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
# edit terraform.tfvars: set aws_account_id and alert_email

make tf-init
make tf-plan              # review what will be created
make tf-apply             # creates buckets, budget alarm, Glue jobs, catalog, Athena workgroup
make download             # copies the source files into the raw bucket

# Initial load (billable: Glue DPU-hours)
make glue-run JOB=rrl-clean-products
make glue-run JOB=rrl-clean-reviews
make glue-run JOB=rrl-build-curated

# Check every table through Athena (billable: bytes scanned)
.venv/bin/python scripts/run_athena_query.py sql/checks/table_counts.sql

# Monthly loads (billable: Glue DPU-hours)
make glue-run JOB=rrl-split-raw-by-month               # once: raw -> one file per category and month
make reset-months FROM=2022-10 TO=2023-09              # dry run; add YES=1 to delete those months
make replay MONTHS="2022-10 2022-11"                   # deliver months; EventBridge starts each load
make idempotency-test MONTH=2023-01                    # load a month twice, compare everything
make fault-injection-test MONTH=2023-09                # inject bad rows, expect quarantine, then a failed gate; cleans up
```

`make tf-apply` plans, refuses any plan that would delete or replace a
resource (unless `ALLOW_DESTROY=1`), and only then applies that exact plan.

## Teardown

```
make destroy
```

This runs `terraform destroy`, which deletes every resource in this project,
including the buckets and everything stored in them.

## Results

Every number here comes from a command shown with it. The raw run records are
in `benchmarks/`.

### Rows

```
$ python scripts/run_athena_query.py sql/analysis/duplicate_keys.sql     # raw JSON
category        unreadable_lines  review_rows  ...  beyond_first_user_product_time  ...
Electronics                    0     43886944  ...                          478035  ...
Toys_and_Games                 0     16260406  ...                          174056  ...
Video_Games                    0      4624615  ...                           53653  ...

$ python scripts/run_athena_query.py sql/checks/table_counts.sql         # loaded tables
table_name                    category        row_count
rrl_cleaned.products          Electronics       1610012
rrl_cleaned.products          Toys_and_Games     890874
rrl_cleaned.products          Video_Games        137269
rrl_cleaned.reviews           Electronics      43408909
rrl_cleaned.reviews           Toys_and_Games   16086350
rrl_cleaned.reviews           Video_Games       4570962
rrl_curated.category_daily    Electronics          8779
rrl_curated.category_daily    Toys_and_Games       8352
rrl_curated.category_daily    Video_Games          8731
rrl_curated.category_monthly  Electronics           298
rrl_curated.category_monthly  Toys_and_Games        286
rrl_curated.category_monthly  Video_Games           297
rrl_curated.product_monthly   Electronics      11646626
rrl_curated.product_monthly   Toys_and_Games    6303793
rrl_curated.product_monthly   Video_Games       1298057
rrl_curated.reviews_enriched  Electronics      43408909
rrl_curated.reviews_enriched  Toys_and_Games   16086350
rrl_curated.reviews_enriched  Video_Games       4570962
```

| | Rows |
|---|---|
| Raw review lines | 64,771,965 |
| Duplicate copies removed (key: user, product, timestamp) | 705,744 (1.09%) |
| Cleaned reviews | 64,066,221 |
| Reviews with no matching product | 0 |
| Products | 2,638,155 |

Raw minus duplicates equals cleaned in every category, and the Glue jobs'
own metrics records (`s3://<artifacts>/metrics/`) report the same counts.
Why that de-duplication key: [docs/decisions.md](docs/decisions.md#de-duplication-key-category-user_id-parent_asin-timestamp).

### Initial load on Glue

Glue 6.0, FLEX execution class, G.1X workers with auto scaling. DPU-seconds
are Glue's own billing figure (`GetJobRun`); cost is DPU-hours x $0.203.

```
$ make glue-run JOB=<job> ARGS="--arg categories=<list>"
```

| Job | Categories | Run time (s) | DPU-seconds | Cost (USD) |
|---|---|---|---|---|
| `rrl-clean-products` | Video_Games | 107 | 227 | 0.0128 |
| `rrl-clean-reviews` | Video_Games | 307 | 1,156 | 0.0652 |
| `rrl-clean-products` | Electronics, Toys_and_Games | 250 | 524 | 0.0295 |
| `rrl-clean-reviews` | Electronics, Toys_and_Games | 2,629 | 13,450 | 0.7584 |
| `rrl-build-curated` | all | 287 | 2,265 | 0.1277 |
| **Total** | | | **17,622** | **0.9936** |

The Electronics and Toys review run is slow for a known reason: the 6.47 GB
Electronics file is one gzip stream, read by a single task for 25 minutes,
and part of that read was repeated after a FLEX worker was lost.
[docs/decisions.md](docs/decisions.md#what-the-first-full-run-taught-gzip-one-task-and-flex)
has the timeline.

### Athena

| Query | Data scanned | Time (s) | Cost (USD) |
|---|---|---|---|
| `sql/analysis/duplicate_keys.sql` (raw gzipped JSON) | 9,237,555,655 bytes | 462.1 | 0.046188 |
| `sql/analysis/duplicate_differences.sql` (raw gzipped JSON) | 9,237,555,655 bytes | 337.5 | 0.046188 |
| `sql/analysis/repeat_reviews.sql` (raw gzipped JSON) | 9,237,555,655 bytes | 330.2 | 0.046188 |
| `sql/checks/table_counts.sql` (Parquet) | 210,232 bytes | 5.0 | 0.000052 |

Counting rows in Parquet reads only file footers; that is why the last query
scanned 210 KB. Like-for-like JSON versus Parquet comparisons are a phase 7
benchmark.

### Incremental monthly loads

The last twelve months of data (2022-10 to 2023-09) were deleted from the
cleaned and curated layers, then delivered again one month at a time through
EventBridge and Step Functions.

**Monthly loads give exactly what the full load gave.** The per-month counts
in every table, saved before the reset and measured again after the replay,
are identical:

```
$ python scripts/run_athena_query.py sql/checks/window_counts.sql --param from=2022-10 --param to=2023-09
$ diff benchmarks/window_counts_backfill.txt benchmarks/window_counts_replay.txt    # result tables: no difference
review_month  cleaned_reviews  distinct_review_ids  enriched_reviews  category_monthly_reviews  ...  rating_sum
2022-10                582447               582447            582447                    582447  ...     2324543
...
2023-09                  5540                 5540              5540                      5540  ...       22276
```

(Full tables: `benchmarks/window_counts_backfill.txt` and
`benchmarks/window_counts_replay.txt`. Only the last line of each, Athena's
run statistics, differs.)

**Loading the same month twice changes nothing:**

```
$ make idempotency-test MONTH=2023-01
loading 2023-01 twice
  run 1: SUCCEEDED in 410s (idempotency-2023-01-1-1794308867)
  run 2: SUCCEEDED in 367s (idempotency-2023-01-2-1794309320)

measure                                             after run 1   after run 2  same
rrl_curated.category_monthly: sum of review_count        773397        773397  yes
rrl_cleaned.reviews: rows in month                       773397        773397  yes
rrl_curated.category_daily: sum of review_count          773397        773397  yes
rrl_curated.reviews_enriched: rows in month              773397        773397  yes
rrl_curated.product_monthly: sum of review_count         773397        773397  yes
rrl_cleaned.reviews: rows in all months                64066221      64066221  yes
rrl_cleaned.reviews: distinct review_id in month         773397        773397  yes
s3 cleaned/reviews/: files in month                           3             3  yes
s3 curated/reviews_enriched/: files in month                  3             3  yes
s3 curated/category_monthly/: files in month                  1             1  yes
s3 curated/category_daily/: files in month                    1             1  yes
s3 curated/product_monthly/: files in month                   1             1  yes

PASS: the second load changed nothing
```

**Cost and run time per monthly load**, read back from Glue (both jobs added
together; FLEX at $0.203 per DPU-hour):

```
$ python scripts/glue_run_costs.py rrl-clean-reviews rrl-build-curated --since 2026-11-10T10:30 --by-month
```

| Month | Reviews loaded | Glue run time (s) | DPU-seconds | Cost (USD) |
|---|---|---|---|---|
| 2022-10 | 582,447 | 291 | 1,112 | 0.0627 |
| 2022-11 | 585,668 | 245 | 980 | 0.0552 |
| 2022-12 | 744,085 | 263 | 1,056 | 0.0595 |
| 2023-01 | 773,397 | 192 | 925 | 0.0522 |
| 2023-02 | 567,520 | 232 | 787 | 0.0443 |
| 2023-03 | 642,719 | 164 | 768 | 0.0433 |
| 2023-04 | 389,756 | 206 | 635 | 0.0358 |
| 2023-05 | 219,961 | 174 | 729 | 0.0411 |
| 2023-06 | 144,841 | 149 | 389 | 0.0219 |
| 2023-07 | 121,582 | 224 | 870 | 0.0490 |
| 2023-08 | 83,423 | 172 | 596 | 0.0336 |
| 2023-09 | 5,540 | 203 | 695 | 0.0392 |
| **Total** | **4,860,939** | | **9,542** | **0.5378** |

Cost barely follows volume: 2023-09 has under 1% of 2023-01's reviews and costs 75%
as much. Most of a small run is fixed overhead (starting Spark, listing
folders, the minimum two workers), not data.

Other phase 3 runs: the one-time split, `rrl-split-raw-by-month` (STANDARD,
$0.308 per DPU-hour), took 1,625 s and 3,251 DPU-seconds ($0.2781) and wrote
64,771,965 lines into 881 files. The two idempotency-test loads cost $0.1225
(`benchmarks/idempotency_glue_runs.jsonl`).

**Retries and alerts were exercised for real.** Eleven months were delivered
at once into four Glue slots. Seven of them were turned away by Glue
(`Glue.ConcurrentRunsExceededException`, 2 to 5 times each, 24 in total),
waited in the state machine's retry loop, and then succeeded. An execution
started with an invalid month (`202301`) went
HasMonth -> CheckMonth -> BadMonth -> NotifyFailure -> LoadFailed, and SNS
delivered the alert email.

### Data quality

Cleaning jobs send every row that breaks a rule to the quarantine bucket with
the reason; `check_quality` then runs after each monthly load and fails it if
the layers do not reconcile or a rule was broken
([docs/decisions.md](docs/decisions.md#phase-5-data-quality) explains both
layers; [docs/data-lake-layout.md](docs/data-lake-layout.md#quarantine) lists
the rules).

**What the real data contained.** Before the rules existed,
`sql/checks/quality_baseline.sql` over every cleaned review found exactly one
kind of problem: two Electronics reviews with a rating of 0. After their
months were re-loaded, both are in quarantine and nothing else is:

```
$ python scripts/run_athena_query.py sql/checks/quarantine_counts.sql
category     load_month  reason               quarantined_rows
Electronics  2022-04     rating_out_of_range                 1
Electronics  2022-12     rating_out_of_range                 1
```

Cleaned Electronics reviews went from 43,408,909 to 43,408,907 (64,066,219 in
all three categories), and `reviews_enriched` matches.

**The gate, proven on AWS.** `make fault-injection-test MONTH=2023-09` adds a
file of made-up bad lines (six kinds, one per rule) to the month's landing
folder and loads the month through the real state machine three times:

```
$ make fault-injection-test MONTH=2023-09
```

| Step | Bad lines added | Landing lines | Quarantined | Cleaned | Execution | Failed check |
|---|---|---|---|---|---|---|
| few | 20 | 5,774 | 20 (0.35%) | 5,540 | SUCCEEDED | none |
| many | 200 | 5,954 | 200 (3.4%) | 5,540 | FAILED at `CheckQuality` | quarantine rate (limit 1%) |
| removed | 0 | 5,754 | 0 | 5,540 | SUCCEEDED | none |

Every injected line was quarantined under the reason it was built to break
(for example, in the "few" step: 4 `unparseable_json`, 4 `missing_key`, 3 of
each other reason), none reached the cleaned table, and the failed execution
went CheckQuality -> NotifyFailure -> LoadFailed: the check was not retried
(a failed check is not transient) and the alert was published to SNS. Because the re-run deletes the month's old quarantine
rows first, the "removed" step leaves the month exactly as before the test.
The nine Glue runs cost $0.1164 (`scripts/glue_run_costs.py`); the full
record is `benchmarks/fault_injection_runs.jsonl`.
