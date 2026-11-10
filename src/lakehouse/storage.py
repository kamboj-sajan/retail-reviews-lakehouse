"""Reading the raw layer and writing partitioned Parquet."""
from lakehouse.schemas import CORRUPT_COLUMN, RAW_PRODUCT_SCHEMA, RAW_REVIEW_SCHEMA


def raw_paths(root, categories=None, month=None):
    """The folders to read under root.

    Everything, some categories, or one month (of every category or some).
    A month is only available where the data is laid out by month: the
    landing area written by the monthly replay, and the cleaned layer.
    """
    if month:
        return [f"{root}/category={c}/review_month={month}" for c in (categories or ["*"])]
    if not categories:
        return [root]
    return [f"{root}/category={c}" for c in categories]


def _read_raw_json(spark, root, schema, categories, month=None):
    return (
        spark.read.schema(schema)
        # PERMISSIVE keeps unparseable lines instead of failing the job or
        # silently dropping them; their text goes into CORRUPT_COLUMN.
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        # The folders are named category=<name>/. Spark turns that folder
        # name into a `category` column on every row (partition discovery);
        # the records themselves do not carry a category. basePath tells Spark
        # where the partitioned layout starts, so the column still appears
        # when only some category folders are read.
        .option("basePath", root)
        .json(raw_paths(root, categories, month))
    )


def read_raw_reviews(spark, raw_path, categories=None, month=None):
    """Read <raw_path>/reviews/category=*/ (optionally some categories, or one month)."""
    return _read_raw_json(spark, f"{raw_path}/reviews", RAW_REVIEW_SCHEMA, categories, month)


def read_raw_review_lines(spark, raw_path, categories=None):
    """Read raw review files as plain text: one row per line, `value` and `category`.

    Used by the monthly split, which must copy each line exactly as it is in
    the source file rather than parse it and write it back out.
    """
    root = f"{raw_path}/reviews"
    return spark.read.option("basePath", root).text(raw_paths(root, categories))


def read_raw_products(spark, raw_path, categories=None):
    """Read <raw_path>/meta/category=*/*.jsonl.gz (optionally some categories only)."""
    return _read_raw_json(spark, f"{raw_path}/meta", RAW_PRODUCT_SCHEMA, categories)


def read_table(spark, table_path, month=None, by_category=True):
    """Read a partitioned Parquet table: all of it, or one month's folders only.

    With a month, only that month's folders are listed. Months may be loaded
    in parallel, and listing the whole table could catch another run in the
    middle of replacing its folders. Listing one month's folders is also far
    quicker than listing all of them.
    """
    if not month:
        return spark.read.parquet(table_path)
    pattern = f"{table_path}/category=*/review_month={month}" if by_category else f"{table_path}/review_month={month}"
    return spark.read.option("basePath", table_path).parquet(pattern)


def delete_paths(spark, pattern):
    """Delete every file or folder matching a glob pattern, local or on S3. Returns how many."""
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    matches = fs.globStatus(hadoop_path) or []
    for match in matches:
        fs.delete(match.getPath(), True)
    return len(matches)


def write_quarantine(spark, df, path, partition_columns, clear_patterns):
    """Replace this run's quarantine rows.

    Dynamic partition overwrite only replaces partitions that appear in the
    data. A re-run that finds no bad rows would therefore leave the previous
    run's quarantine rows in place, and they would be counted twice. So the
    partitions this run is responsible for are deleted first.
    """
    for pattern in clear_patterns:
        delete_paths(spark, pattern)
    df.write.mode("overwrite").partitionBy(*partition_columns).parquet(path)


def parse_categories(text):
    """'A,B' -> ['A', 'B']; empty or None -> None (meaning every category)."""
    return [c.strip() for c in text.split(",") if c.strip()] if text else None


def month_argument(text):
    """argparse type for --month: accepts YYYY-MM only."""
    import argparse
    import re

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", text or ""):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {text!r}")
    return text


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
