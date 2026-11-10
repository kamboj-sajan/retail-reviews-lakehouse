"""Job: raw review JSON -> cleaned Parquet, partitioned by category and month.

Local run:
    PYTHONPATH=src python jobs/clean_reviews.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
"""
import argparse

from lakehouse.session import build_spark
from lakehouse.storage import read_raw_reviews, write_partitioned
from lakehouse.transforms import clean_reviews


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    # parse_known_args: Glue adds arguments of its own (--JOB_NAME and others).
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("clean_reviews")
    target = f"{args.cleaned_path}/reviews"
    write_partitioned(clean_reviews(read_raw_reviews(spark, args.raw_path)), target, ["category", "review_month"])

    # Counting Parquet is cheap: Spark reads the row counts stored in each
    # file's footer instead of scanning the data.
    print(f"clean_reviews: {spark.read.parquet(target).count()} rows in {target}")


if __name__ == "__main__":
    main()
