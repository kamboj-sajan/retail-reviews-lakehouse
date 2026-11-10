"""Job: raw product metadata JSON -> cleaned Parquet, one row per product.

Local run:
    PYTHONPATH=src python jobs/clean_products.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.schemas import CORRUPT_COLUMN
from lakehouse.session import build_spark
from lakehouse.storage import read_raw_products, write_partitioned
from lakehouse.transforms import clean_products


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("clean_products")
    run = JobRun(spark, "clean_products", args.metrics_path)
    source = f"{args.raw_path}/meta"
    target = f"{args.cleaned_path}/products"

    raw = run.observe(
        read_raw_products(spark, args.raw_path),
        raw_lines=F.count(F.lit(1)),
        corrupt_lines=F.count(F.col(CORRUPT_COLUMN)),
        rows_missing_key=F.count(F.when(F.col("parent_asin").isNull() & F.col(CORRUPT_COLUMN).isNull(), 1)),
    )
    # Products have no date, so category is the only partition column.
    write_partitioned(clean_products(raw), target, ["category"])

    written = spark.read.parquet(target).count()
    counts = run.observed()
    run.record(
        written_rows=written,
        duplicate_rows_removed=counts["raw_lines"] - counts["corrupt_lines"] - counts["rows_missing_key"] - written,
        input_bytes=path_bytes(spark, source),
        output_bytes=path_bytes(spark, target),
    )
    run.save()


if __name__ == "__main__":
    main()
