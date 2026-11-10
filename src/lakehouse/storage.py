"""Reading the raw layer and writing partitioned Parquet."""
from lakehouse.schemas import CORRUPT_COLUMN, RAW_PRODUCT_SCHEMA, RAW_REVIEW_SCHEMA


def _read_raw_json(spark, path, schema):
    return (
        spark.read.schema(schema)
        # PERMISSIVE keeps unparseable lines instead of failing the job or
        # silently dropping them; their text goes into CORRUPT_COLUMN.
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        # `path` contains folders named category=<name>/. Spark turns that
        # folder name into a `category` column on every row (partition
        # discovery). The review records themselves do not carry a category.
        .json(path)
    )


def read_raw_reviews(spark, raw_path):
    """Read <raw_path>/reviews/category=*/*.jsonl.gz."""
    return _read_raw_json(spark, f"{raw_path}/reviews", RAW_REVIEW_SCHEMA)


def read_raw_products(spark, raw_path):
    """Read <raw_path>/meta/category=*/*.jsonl.gz."""
    return _read_raw_json(spark, f"{raw_path}/meta", RAW_PRODUCT_SCHEMA)


def write_partitioned(df, path, partition_columns, max_records_per_file=1_000_000):
    """Write df as Parquet under path, one folder per partition value.

    Re-running with the same data replaces the affected partition folders and
    leaves every other partition alone (see partitionOverwriteMode in
    lakehouse/session.py), so a re-run never produces duplicates.
    """
    (
        df
        # Without this, every Spark task writes its own file into every
        # partition folder it has rows for: hundreds of tasks times hundreds
        # of months gives a flood of tiny files, which are slow to list and
        # slow to query. repartition() shuffles rows so that all rows of one
        # partition value sit in one task, and that task writes one file.
        .repartition(*partition_columns)
        .write.mode("overwrite")
        .partitionBy(*partition_columns)
        # Safety valve for very large partitions: start a new file after this
        # many rows, so no single file grows without bound.
        .option("maxRecordsPerFile", max_records_per_file)
        .parquet(path)
    )
