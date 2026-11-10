"""Job: raw review JSON -> cleaned Parquet, typed and de-duplicated.

Two modes:
- backfill (no --month): read the original raw files, write every month;
- monthly (--month YYYY-MM): read only that month's folder from the landing
  area and replace only that month's partitions. Every copy of a review has
  the same timestamp, so all copies are in the same month and de-duplicating
  one month at a time is complete.

Local runs:
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/local/landing --cleaned_path data/local/cleaned --month 2022-11
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.schemas import CORRUPT_COLUMN
from lakehouse.session import build_spark
from lakehouse.storage import (month_argument, parse_categories, raw_paths, read_raw_reviews, read_table,
                               write_partitioned)
from lakehouse.transforms import REVIEW_KEY, deduplicate_reviews, type_reviews


def main(argv=None):
    # allow_abbrev=False: Glue adds arguments of its own, and argparse would
    # otherwise accept any prefix of ours as if it were the full name.
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer, or of the landing area with --month")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    # Only these categories' folders are read, and only their partitions are
    # replaced in the output; every other category is left untouched.
    parser.add_argument("--categories", help="comma-separated; default: every category")
    parser.add_argument("--month", type=month_argument, help="process only this month (YYYY-MM)")
    # parse_known_args: ignore the arguments Glue adds (--JOB_NAME and others).
    args, _ = parser.parse_known_args(argv)
    categories = parse_categories(args.categories)
    month = args.month

    spark = build_spark("clean_reviews")
    run = JobRun(spark, "clean_reviews", args.metrics_path)
    source = f"{args.raw_path}/reviews"
    target = f"{args.cleaned_path}/reviews"

    raw = run.observe(
        read_raw_reviews(spark, args.raw_path, categories, month),
        raw_lines=F.count(F.lit(1)),
        corrupt_lines=F.count(F.col(CORRUPT_COLUMN)),  # count() skips NULLs
    )
    key_present = F.lit(True)
    for column in REVIEW_KEY:
        key_present = key_present & F.col(column).isNotNull()
    typed = type_reviews(raw)
    if month:
        # Safety rail for incremental runs. Dynamic partition overwrite
        # replaces every partition the output touches, so a single row with
        # another month's timestamp would wipe that other month and leave
        # only this row in it. Such rows are counted and never written.
        in_month = F.coalesce(F.col("review_month") == F.lit(month), F.lit(False))
        typed = run.observe(typed, rows_outside_month=F.count(F.when(~in_month, 1))).where(in_month)
    typed = run.observe(
        typed,
        parsed_rows=F.count(F.lit(1)),
        rows_missing_key=F.count(F.when(~key_present, 1)),
    )
    write_partitioned(deduplicate_reviews(typed), target, ["category", "review_month"])

    # Count only what this run wrote. Counting Parquet is cheap: Spark reads
    # the row count stored in each file's footer instead of scanning the data.
    output = read_table(spark, target, month)
    if categories:
        output = output.where(F.col("category").isin(categories))
    written = output.count()
    counts = run.observed()
    run.record(
        categories=",".join(categories) if categories else "all",
        month=month or "all",
        written_rows=written,
        duplicate_rows_removed=counts["parsed_rows"] - written,
        input_bytes=sum(path_bytes(spark, p) for p in raw_paths(source, categories, month)),
        output_bytes=sum(path_bytes(spark, p) for p in raw_paths(target, categories, month)),
    )
    run.save()


if __name__ == "__main__":
    main()
