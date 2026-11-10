"""Run the three jobs end to end on the local sample in data/sample/raw.

Skipped when the sample has not been downloaded (run `make sample` first).
Expected counts are computed independently in plain Python, so Spark is not
being checked against itself.
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


def plain_python_counts(kind, key_fields):
    """(valid lines, distinct keys) over every sample file, without Spark."""
    lines, keys = 0, set()
    for path in glob.glob(os.path.join(SAMPLE_RAW, kind, "category=*", "*.jsonl.gz")):
        category = os.path.basename(os.path.dirname(path)).split("=", 1)[1]
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    record = json.loads(line)
                    lines += 1
                    keys.add((category,) + tuple(record[f] for f in key_fields))
    return lines, len(keys)


def test_jobs_on_sample(spark, tmp_path):
    cleaned_path = str(tmp_path / "cleaned")
    curated_path = str(tmp_path / "curated")
    metrics_path = str(tmp_path / "metrics")

    clean_reviews.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path, "--metrics_path", metrics_path])
    clean_products.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path, "--metrics_path", metrics_path])
    build_curated.main(["--cleaned_path", cleaned_path, "--curated_path", curated_path, "--metrics_path", metrics_path])

    review_lines, distinct_reviews = plain_python_counts("reviews", ["user_id", "parent_asin", "timestamp"])
    product_lines, distinct_products = plain_python_counts("meta", ["parent_asin"])

    reviews = spark.read.parquet(f"{cleaned_path}/reviews")
    assert reviews.count() == distinct_reviews
    assert reviews.select("review_id").distinct().count() == distinct_reviews
    assert spark.read.parquet(f"{cleaned_path}/products").count() == distinct_products

    # Key columns survived the type conversion.
    assert reviews.where("rating IS NULL OR review_month IS NULL OR parent_asin IS NULL").count() == 0
    assert reviews.where("rating < 1 OR rating > 5").count() == 0

    # The join keeps every review, and every aggregate counts each review once.
    assert spark.read.parquet(f"{curated_path}/reviews_enriched").count() == distinct_reviews
    for table in ("category_monthly", "category_daily", "product_monthly"):
        total = spark.read.parquet(f"{curated_path}/{table}").agg({"review_count": "sum"}).first()[0]
        assert total == distinct_reviews, table

    # The metrics records agree with the independent counts.
    metrics = {r["job"]: r for r in spark.read.json(metrics_path).collect()}
    assert metrics["clean_reviews"]["raw_lines"] == review_lines
    assert metrics["clean_reviews"]["corrupt_lines"] == 0
    assert metrics["clean_reviews"]["duplicate_rows_removed"] == review_lines - distinct_reviews
    assert metrics["clean_products"]["written_rows"] == distinct_products
    assert metrics["build_curated"]["reviews_enriched_rows"] == distinct_reviews

    # Running a job a second time changes nothing.
    clean_reviews.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned_path])
    assert spark.read.parquet(f"{cleaned_path}/reviews").count() == distinct_reviews
