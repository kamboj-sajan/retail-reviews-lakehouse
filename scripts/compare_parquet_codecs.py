#!/usr/bin/env python3
"""Measure how big the cleaned reviews are under each Parquet compression codec.

This is the evidence behind choosing zstd (docs/decisions.md). It rewrites the
cleaned sample reviews with snappy, zstd and gzip in two layouts:

  by month      partitioned by category and review_month, like the real table.
                On the small sample that is hundreds of tiny files, so the
                per-file overhead of Parquet is a large share of the total.
  by category   one file per category. Shows the codec's effect with the
                small-file overhead removed.

Run `make local-pipeline` first, then:
    PYTHONPATH=src .venv/bin/python scripts/compare_parquet_codecs.py
"""
import os
import shutil
import tempfile

from pyspark.sql import SparkSession

from lakehouse.session import build_spark

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RAW_REVIEWS = os.path.join(ROOT, "data", "sample", "raw", "reviews")
CLEANED_REVIEWS = os.path.join(ROOT, "data", "local", "cleaned", "reviews")


def total_bytes(folder, suffix):
    """Sum of file sizes. (du would round every tiny file up to a disk block.)"""
    return sum(
        os.path.getsize(os.path.join(directory, name))
        for directory, _, names in os.walk(folder)
        for name in names
        if name.endswith(suffix)
    )


def main():
    SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    spark = build_spark("compare_parquet_codecs")
    spark.sparkContext.setLogLevel("ERROR")

    reviews = spark.read.parquet(CLEANED_REVIEWS).cache()
    rows = reviews.count()
    raw_bytes = total_bytes(RAW_REVIEWS, ".gz")
    scratch = tempfile.mkdtemp(prefix="codec-compare-")

    print(f"rows: {rows}")
    print(f"{'layout':12} {'codec':8} {'bytes':>12} {'vs raw gzip JSON':>18}")
    print(f"{'raw JSON':12} {'gzip':8} {raw_bytes:>12} {1.0:>17.2f}x")
    try:
        for codec in ("snappy", "zstd", "gzip"):
            by_month = os.path.join(scratch, f"month-{codec}")
            (reviews.repartition("category", "review_month")
             .write.partitionBy("category", "review_month").option("compression", codec).parquet(by_month))
            size = total_bytes(by_month, ".parquet")
            print(f"{'by month':12} {codec:8} {size:>12} {size / raw_bytes:>17.2f}x")

            by_category = os.path.join(scratch, f"category-{codec}")
            (reviews.repartition("category")
             .write.partitionBy("category").option("compression", codec).parquet(by_category))
            size = total_bytes(by_category, ".parquet")
            print(f"{'by category':12} {codec:8} {size:>12} {size / raw_bytes:>17.2f}x")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
