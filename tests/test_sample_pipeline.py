"""Run the three jobs end to end on the local sample in data/sample/raw.

Skipped when the sample has not been downloaded (run `make sample` first).
Row counts are compared against an independent count made in plain Python,
so Spark is not being checked against itself.
"""
import glob
import gzip
import json
import os

import pytest

import build_curated
import clean_products
import clean_reviews

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SAMPLE_RAW = os.path.join(ROOT, "data", "sample", "raw")

pytestmark = pytest.mark.skipif(
    not glob.glob(os.path.join(SAMPLE_RAW, "reviews", "category=*", "*.jsonl.gz")),
    reason="no local sample; run `make sample`",
)


def count_valid_json_lines(pattern):
    """Count lines that parse as JSON, without Spark."""
    total = 0
    for path in glob.glob(pattern):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    json.loads(line)
                    total += 1
    return total


def test_jobs_on_sample(spark, tmp_path):
    cleaned_path = str(tmp_path / "cleaned")
    curated_path = str(tmp_path / "curated")

    clean_reviews.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path])
    clean_products.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path])
    build_curated.main(["--cleaned_path", cleaned_path, "--curated_path", curated_path])

    # Nothing is filtered or de-duplicated yet, so every raw line must arrive.
    reviews = spark.read.parquet(f"{cleaned_path}/reviews")
    products = spark.read.parquet(f"{cleaned_path}/products")
    raw_review_lines = count_valid_json_lines(os.path.join(SAMPLE_RAW, "reviews", "category=*", "*.jsonl.gz"))
    raw_product_lines = count_valid_json_lines(os.path.join(SAMPLE_RAW, "meta", "category=*", "*.jsonl.gz"))
    assert reviews.count() == raw_review_lines
    assert products.count() == raw_product_lines

    # Key columns survived the type conversion.
    assert reviews.where("rating IS NULL OR review_month IS NULL OR parent_asin IS NULL").count() == 0
    assert reviews.where("rating < 1 OR rating > 5").count() == 0

    # Every aggregate table accounts for every review exactly once.
    for table in ("category_monthly", "category_daily", "product_monthly"):
        total = spark.read.parquet(f"{curated_path}/{table}").agg({"review_count": "sum"}).collect()[0][0]
        assert total == raw_review_lines, table

    # Running a job a second time changes nothing.
    clean_reviews.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path])
    assert spark.read.parquet(f"{cleaned_path}/reviews").count() == raw_review_lines
