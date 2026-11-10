"""Job: raw product metadata JSON -> cleaned Parquet, partitioned by category.

Local run:
    PYTHONPATH=src python jobs/clean_products.py --raw_path data/sample/raw --cleaned_path data/local/cleaned
"""
import argparse

from lakehouse.session import build_spark
from lakehouse.storage import read_raw_products, write_partitioned
from lakehouse.transforms import clean_products


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw_path", required=True, help="root of the raw layer")
    parser.add_argument("--cleaned_path", required=True, help="root of the cleaned layer")
    args, _ = parser.parse_known_args(argv)

    spark = build_spark("clean_products")
    target = f"{args.cleaned_path}/products"
    # Products have no date, so category is the only partition column.
    write_partitioned(clean_products(read_raw_products(spark, args.raw_path)), target, ["category"])

    print(f"clean_products: {spark.read.parquet(target).count()} rows in {target}")


if __name__ == "__main__":
    main()
