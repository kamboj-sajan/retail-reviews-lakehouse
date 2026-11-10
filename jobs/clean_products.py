"""Job: raw product metadata JSON -> cleaned Parquet, one row per product.

Local run:
    PYTHONPATH=src python jobs/clean_products.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
"""
import argparse

from pyspark.sql import functions as F

from lakehouse.metrics import JobRun, path_bytes
from lakehouse.schemas import CORRUPT_COLUMN
from lakehouse.session import build_spark
from lakehouse.storage import parse_categories, raw_paths, read_raw_products, write_partitioned
from lakehouse.transforms import clean_products


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    parser.add_argument("--metrics_path", help="where to write this run's metrics record")
    # Only these categories' folders are read, and only their partitions are
    # replaced in the output; every other category is left untouched.
    parser.add_argument("--categories", help="comma-separated; default: every category")
    args, _ = parser.parse_known_args(argv)
    categories = parse_categories(args.categories)

    spark = build_spark("clean_products")
    run = JobRun(spark, "clean_products", args.metrics_path)
    source = f"{args.raw_path}/meta"
    target = f"{args.cleaned_path}/products"

    raw = run.observe(
        read_raw_products(spark, args.raw_path, categories),
        raw_lines=F.count(F.lit(1)),
        corrupt_lines=F.count(F.col(CORRUPT_COLUMN)),
        rows_missing_key=F.count(F.when(F.col("parent_asin").isNull() & F.col(CORRUPT_COLUMN).isNull(), 1)),
    )
    # Products have no date, so category is the only partition column.
    write_partitioned(clean_products(raw), target, ["category"])

    output = spark.read.parquet(target)
    if categories:
        output = output.where(F.col("category").isin(categories))
    written = output.count()
    counts = run.observed()
    run.record(
        categories=",".join(categories) if categories else "all",
        written_rows=written,
        duplicate_rows_removed=counts["raw_lines"] - counts["corrupt_lines"] - counts["rows_missing_key"] - written,
        input_bytes=sum(path_bytes(spark, p) for p in raw_paths(source, categories)),
        output_bytes=sum(path_bytes(spark, p) for p in raw_paths(target, categories)),
    )
    run.save()


if __name__ == "__main__":
    main()
