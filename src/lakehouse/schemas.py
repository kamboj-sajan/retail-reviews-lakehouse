"""Schemas for reading the raw JSON files.

Why the schema is written out instead of inferred
-------------------------------------------------
Spark can infer a JSON schema, but to do so it must read every file an extra
time, and here that means decompressing 11 GB twice. Inference is also unsafe
for this data: the product `details` object has hundreds of different keys
(docs/schema.md), and Spark would turn each one into its own column.

Only the fields listed here are parsed. Any other field in the JSON is ignored.

Why some fields are read as wider types than they "should" be
------------------------------------------------------------
If a value does not fit the declared type, Spark marks the record as corrupt.
So raw fields are read leniently (long instead of int, string for `price`) and
narrowed in lakehouse/transforms.py, where a bad value becomes a NULL that can
be counted instead of a lost row.
"""
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# When a line cannot be parsed, Spark puts the original text in this column
# and leaves it NULL for good lines. Phase 5 sends those lines to quarantine.
CORRUPT_COLUMN = "_corrupt_record"

RAW_REVIEW_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("rating", DoubleType()),          # JSON holds 5.0, not 5
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("timestamp", LongType()),         # Unix time in MILLISECONDS
    StructField("helpful_vote", LongType()),
    StructField("verified_purchase", BooleanType()),
    # Each image is a JSON object. Declaring the element as a string makes
    # Spark keep each object as its JSON text; only the count is used.
    StructField("images", ArrayType(StringType())),
    StructField(CORRUPT_COLUMN, StringType()),
])

RAW_PRODUCT_SCHEMA = StructType([
    StructField("parent_asin", StringType()),
    StructField("title", StringType()),
    StructField("main_category", StringType()),
    StructField("store", StringType()),
    # Read as text and converted with try_cast: a non-numeric price becomes
    # NULL instead of making the whole product record corrupt.
    StructField("price", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", LongType()),
    StructField("categories", ArrayType(StringType())),
    # An object with hundreds of possible keys. Declared as a string, so Spark
    # stores the object's JSON text in one column.
    StructField("details", StringType()),
    StructField(CORRUPT_COLUMN, StringType()),
])
