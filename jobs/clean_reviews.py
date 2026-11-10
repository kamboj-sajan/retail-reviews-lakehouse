"""Job: raw review JSON -> cleaned Parquet, typed and de-duplicated; bad rows -> quarantine.

Two modes:
- backfill (no --month): read every category folder under --raw_path and
  write every month;
- monthly (--month YYYY-MM): read only that month's folder from the landing
  area and replace only that month's partitions. Every copy of a review has
  the same timestamp, so all copies are in the same month and de-duplicating
  one month at a time is complete.

Every input row ends up in exactly one place: cleaned, quarantine, or counted
as a duplicate copy. The metrics record shows all three, and the quality
check (jobs/check_quality.py) verifies that they add up.

Local runs:
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/sample/raw --cleaned_path data/local/cleaned --quarantine_path data/local/quarantine
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/local/landing --cleaned_path data/local/cleaned --quarantine_path data/local/quarantine --month 2022-11
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.session import build_spark
from lakehouse.storage import (month_argument, parse_categories, raw_paths, read_raw_reviews, read_table,
                               write_partitioned, write_quarantine)
from lakehouse.transforms import REVIEW_PROBLEMS, count_by_reason, deduplicate_reviews, split_reviews, type_reviews


def main(argv=None):
    # allow_abbrev=False: Glue adds arguments of its own, and argparse would
    # otherwise accept any prefix of ours as if it were the full name.
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer, or of the landing area with --month")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--quarantine_path", required=True, help="root of the quarantine bucket")
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
    quarantine = f"{args.quarantine_path}/reviews"

    raw = run.observe(read_raw_reviews(spark, args.raw_path, categories, month), raw_lines=F.count(F.lit(1)))
    loadable, rejected = split_reviews(raw, month)

    # The source is read twice: once for the (few) quarantined rows and once
    # for the rows to load. That is cheap for a month's files, or for the
    # split feed (881 small files read in parallel), but means re-reading a
    # whole gzip stream if a backfill points at the original downloads.
    if month:
        clear = [f"{quarantine}/category={c}/load_month={month}" for c in (categories or ["*"])]
    else:
        clear = [f"{quarantine}/category={c}" for c in categories] if categories else [quarantine]
    write_quarantine(spark, run.observe(rejected, **count_by_reason(REVIEW_PROBLEMS)),
                     quarantine, ["category", "load_month"], clear)

    typed = run.observe(type_reviews(loadable), parsed_rows=F.count(F.lit(1)))
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
    record = run.save()
    # Every input row is accounted for exactly once.
    accounted = record["written_rows"] + record["duplicate_rows_removed"] + record["quarantined_rows"]
    if accounted != record["raw_lines"]:
        raise RuntimeError(f"row accounting does not balance: {record['raw_lines']} lines read, {accounted} accounted for")


if __name__ == "__main__":
    main()
