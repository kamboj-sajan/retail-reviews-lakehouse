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
| 1 | Data lake layers: raw, cleaned, curated | Layout defined; jobs pass on the local sample. Cleaned and curated are not yet written to S3 (that runs on Glue in phase 2) |
| 2 | PySpark ETL on Glue, Glue Data Catalog | Not started |
| 3 | Incremental monthly loads with Step Functions and EventBridge | Not started |
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
Those are the authors' figures; this project's own row counts will be measured
in phase 2.

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
jobs/                              job entry points: clean_reviews, clean_products, build_curated
scripts/download_raw.py            copies the source files into the raw S3 bucket
scripts/sample_raw.py              saves a small local sample for development
scripts/inspect_schema.py          profiles the real fields in the sample
scripts/compare_parquet_codecs.py  measures Parquet size per compression codec
tests/                             tests (no AWS access needed)
docs/schema.md                     the schema observed in the real files
docs/data-lake-layout.md           paths, columns and types of every layer
docs/decisions.md                  design decisions and trade-offs
```

## Running it

Requirements: [uv](https://docs.astral.sh/uv/) (it installs Python 3.11, the
version inside AWS Glue 5.1), Java 11 or 17 for Spark, Terraform 1.6+, and AWS
credentials in your environment (for example from `aws configure`). No
credentials are stored in this repository.

Locally, with no AWS access:

```
make venv                 # create .venv: Python 3.11, PySpark 3.5.6, boto3, pytest, moto
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
make tf-apply             # creates the buckets and the budget alarm
make download             # copies the source files into the raw bucket
```

## Teardown

```
make destroy
```

This runs `terraform destroy`, which deletes every resource in this project,
including the buckets and everything stored in them.

## Results

None yet. Every number added here will come with the command that produced it.
