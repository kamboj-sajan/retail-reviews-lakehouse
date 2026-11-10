"""Job: raw review JSON -> cleaned Parquet, typed and de-duplicated.

Local run:
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.schemas import CORRUPT_COLUMN
from lakehouse.session import build_spark
from lakehouse.storage import read_raw_reviews, write_partitioned
from lakehouse.transforms import REVIEW_KEY, deduplicate_reviews, type_reviews


def main(argv=None):
    # allow_abbrev=False: Glue adds arguments of its own, and argparse would
    # otherwise accept any prefix of ours as if it were the full name.
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    # parse_known_args: ignore the arguments Glue adds (--JOB_NAME and others).
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("clean_reviews")
    run = JobRun(spark, "clean_reviews", args.metrics_path)
    source = f"{args.raw_path}/reviews"
    target = f"{args.cleaned_path}/reviews"

    raw = run.observe(
        read_raw_reviews(spark, args.raw_path),
        raw_lines=F.count(F.lit(1)),
        corrupt_lines=F.count(F.col(CORRUPT_COLUMN)),  # count() skips NULLs
    )
    key_present = F.lit(True)
    for column in REVIEW_KEY:
        key_present = key_present & F.col(column).isNotNull()
    typed = run.observe(
        type_reviews(raw),
        parsed_rows=F.count(F.lit(1)),
        rows_missing_key=F.count(F.when(~key_present, 1)),
    )
    write_partitioned(deduplicate_reviews(typed), target, ["category", "review_month"])

    # Counting Parquet is cheap: Spark reads the row count stored in each
    # file's footer instead of scanning the data.
    written = spark.read.parquet(target).count()
    counts = run.observed()
    run.record(
        written_rows=written,
        duplicate_rows_removed=counts["parsed_rows"] - written,
        input_bytes=path_bytes(spark, source),
        output_bytes=path_bytes(spark, target),
    )
    run.save()


if __name__ == "__main__":
    main()
