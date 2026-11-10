"""Incremental (monthly) loads: splitting raw by month, then running one month twice.

Uses the local sample (run `make sample` first). The idempotency test is the
local version of the requirement "run one month twice and prove the row
counts do not change"; scripts/test_month_idempotency.py proves the same on
AWS through Step Functions.
"""
import argparse
import collections
import datetime
import glob
import gzip
import json
import os
import shutil

import pytest

import build_curated
import clean_products
import clean_reviews
import split_raw_by_month
from lakehouse.storage import month_argument

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SAMPLE_RAW = os.path.join(ROOT, "data", "sample", "raw")
needs_sample = pytest.mark.skipif(
    not glob.glob(os.path.join(SAMPLE_RAW, "reviews", "category=*", "*.jsonl.gz")),
    reason="no local sample; run `make sample`",
)


def lines_under(pattern):
    counter = collections.Counter()
    for path in glob.glob(pattern, recursive=True):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            counter.update(line.rstrip("\n") for line in handle if line.strip())
    return counter


def deliver(feed, landing, month):
    """What scripts/replay_month.py does on S3: copy one month into landing."""
    for folder in glob.glob(f"{feed}/reviews/category=*/review_month={month}"):
        category = os.path.basename(os.path.dirname(folder))
        shutil.copytree(folder, f"{landing}/reviews/{category}/review_month={month}", dirs_exist_ok=True)


def files(root):
    return sorted(p for p in glob.glob(f"{root}/**/*.parquet", recursive=True))


def test_month_argument_accepts_only_yyyy_mm():
    assert month_argument("2023-01") == "2023-01"
    for bad in ("2023-1", "2023-13", "23-01", "2023/01", "", None):
        with pytest.raises(argparse.ArgumentTypeError):
            month_argument(bad)


@needs_sample
def test_split_copies_every_line_unchanged_into_its_month(spark, tmp_path):
    feed = str(tmp_path / "feed")
    split_raw_by_month.main(["--raw_path", SAMPLE_RAW, "--feed_path", feed])

    original = lines_under(f"{SAMPLE_RAW}/reviews/category=*/*.jsonl.gz")
    split = lines_under(f"{feed}/reviews/category=*/review_month=*/*.gz")
    assert split == original  # same lines, same number of times, byte for byte

    # One file per category and month: each raw file is read by one task,
    # which writes each of its months exactly once.
    folders = glob.glob(f"{feed}/reviews/category=*/review_month=*")
    assert all(len(glob.glob(f"{f}/*.gz")) == 1 for f in folders)

    # A line sits in the folder of its own month.
    sample_folder = sorted(folders)[len(folders) // 2]
    month = sample_folder.rsplit("=", 1)[1]
    for line in lines_under(f"{sample_folder}/*.gz"):
        ms = json.loads(line)["timestamp"]
        assert datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m") == month


@needs_sample
def test_running_one_month_twice_changes_nothing(spark, tmp_path):
    feed, landing = str(tmp_path / "feed"), str(tmp_path / "landing")
    cleaned, curated = str(tmp_path / "cleaned"), str(tmp_path / "curated")
    split_raw_by_month.main(["--raw_path", SAMPLE_RAW, "--feed_path", feed])

    # History first: the full backfill, exactly as in phase 2.
    clean_reviews.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned])
    clean_products.main(["--raw_path", SAMPLE_RAW, "--cleaned_path", cleaned])
    build_curated.main(["--cleaned_path", cleaned, "--curated_path", curated])
    reviews = spark.read.parquet(f"{cleaned}/reviews")
    month = reviews.groupBy("review_month").count().orderBy("count", ascending=False).first()["review_month"]

    def snapshot():
        cleaned_df = spark.read.parquet(f"{cleaned}/reviews")
        curated_monthly = spark.read.parquet(f"{curated}/category_monthly").where(f"review_month = '{month}'")
        return {
            "cleaned_total": cleaned_df.count(),
            "cleaned_month": cleaned_df.where(f"review_month = '{month}'").count(),
            "enriched_total": spark.read.parquet(f"{curated}/reviews_enriched").count(),
            "monthly_review_count": curated_monthly.agg({"review_count": "sum"}).first()[0],
            "other_months_files": [f for f in files(cleaned) + files(curated) if f"review_month={month}" not in f],
        }

    before = snapshot()
    deliver(feed, landing, month)
    for _ in range(2):  # the same month, twice
        clean_reviews.main(["--raw_path", landing, "--cleaned_path", cleaned, "--month", month])
        build_curated.main(["--cleaned_path", cleaned, "--curated_path", curated, "--month", month])
        assert snapshot() == before


@needs_sample
def test_a_row_from_another_month_is_never_written(spark, tmp_path):
    cleaned = str(tmp_path / "cleaned")
    landing = str(tmp_path / "landing")
    metrics = str(tmp_path / "metrics")
    january = {"user_id": "u1", "asin": "A1", "parent_asin": "P1", "rating": 5.0, "title": "t", "text": "x",
               "timestamp": 1672531200000, "helpful_vote": 0, "verified_purchase": True, "images": []}
    stray_from_february = dict(january, user_id="u2", timestamp=1675209600000)
    folder = f"{landing}/reviews/category=Video_Games/review_month=2023-01"
    os.makedirs(folder)
    with gzip.open(f"{folder}/part.txt.gz", "wt") as handle:
        handle.write(json.dumps(january) + "\n" + json.dumps(stray_from_february) + "\n")

    clean_reviews.main(["--raw_path", landing, "--cleaned_path", cleaned, "--metrics_path", metrics, "--month", "2023-01"])

    written = spark.read.parquet(f"{cleaned}/reviews")
    assert [r["review_month"] for r in written.collect()] == ["2023-01"]
    assert not glob.glob(f"{cleaned}/reviews/category=Video_Games/review_month=2023-02")
    record = spark.read.json(metrics).first()
    assert record["rows_outside_month"] == 1 and record["written_rows"] == 1 and record["month"] == "2023-01"
