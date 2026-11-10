"""Create the SparkSession with the settings every job depends on."""
from pyspark.sql import SparkSession


def build_spark(app_name):
    """Return a SparkSession configured the same way on a laptop and on Glue.

    These settings change the DATA that gets read or written, so they are set
    in code rather than left to whatever the machine's defaults happen to be.
    """
    return (
        SparkSession.builder.appName(app_name)
        # Spark converts timestamps using the session time zone, which
        # defaults to the machine's zone. My laptop is on IST and Glue is on
        # UTC, so without this a review written at 23:50 UTC on 31 January
        # would land in the January partition on Glue and February locally.
        .config("spark.sql.session.timeZone", "UTC")
        # With mode("overwrite"), Spark's default ("static") deletes the WHOLE
        # table before writing. "dynamic" replaces only the partitions present
        # in the data being written. This is what lets one month be re-run
        # without touching the others and without creating duplicates.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Spark's default Parquet timestamp encoding is INT96, a legacy format.
        # TIMESTAMP_MICROS is the standard one that Athena and Redshift read
        # as a normal timestamp column.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Spark compresses Parquet with snappy by default: fast, but weak on
        # long text, and review text is most of this data. On the local sample
        # snappy Parquet was 1.8x the size of the gzipped JSON; zstd was 1.3x
        # (docs/decisions.md has the numbers). Athena reads zstd Parquet.
        .config("spark.sql.parquet.compression.codec", "zstd")
        # When Spark reads folders like review_month=2023-03/, it guesses the
        # column's type from the folder names. Turning the guessing off keeps
        # partition columns as text, exactly as they were written.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
