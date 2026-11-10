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
| 3 | Incremental monthly loads with Step Functions and EventBridge | In progress |
| 4 | Redshift Serverless star schema | Not started |
| 5 | Data quality checks and quarantine | Not started |
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
scripts/sample_raw.py              saves a small local sample for development
scripts/inspect_schema.py          profiles the real fields in the sample
scripts/compare_parquet_codecs.py  measures Parquet size per compression codec
tests/                             tests (no AWS access needed)
benchmarks/                        one JSON line per Glue run and Athena query actually executed
docs/schema.md                     the schema observed in the real files
docs/data-lake-layout.md           paths, columns and types of every layer
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
```

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
