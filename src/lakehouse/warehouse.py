"""The Redshift warehouse: load files, load SQL, and the checks after a load.

The star schema is in sql/warehouse/schema.sql. Loading one table works the
same way every time:

1. Spark writes a load file set: Parquet whose columns are exactly the
   table's columns, in the table's order. COPY from Parquet fills columns by
   position and needs the counts to match, and the lake's own files do not
   (their partition columns live in folder names, not in the files).
2. One Redshift transaction COPYs those files into a temporary staging table,
   deletes the rows being replaced (one month, or the whole table) and
   inserts the staged rows. Readers see the old rows or the new rows, never a
   half-loaded month, and re-running a load replaces instead of duplicating.
3. Queries check that what Redshift now holds is what Spark wrote.

Nothing here talks to AWS except RedshiftData, which wraps the Redshift Data
API (an HTTPS API: no database driver, no network path into the VPC).
"""
import calendar
import datetime
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = "dw"

# Column order and Spark types of every warehouse table. Must match
# sql/warehouse/schema.sql column for column (tests/test_warehouse.py checks).
# The Spark type decides the Parquet type, and COPY needs one it can convert
# to the Redshift column: ShortType for SMALLINT, IntegerType for INTEGER.
COLUMNS = {
    "dim_date": [
        ("date_key", T.IntegerType()), ("full_date", T.DateType()), ("year", T.ShortType()),
        ("quarter", T.ShortType()), ("month", T.ShortType()), ("month_name", T.StringType()),
        ("year_month", T.StringType()), ("day_of_month", T.ShortType()), ("day_of_week", T.ShortType()),
        ("day_name", T.StringType()), ("is_weekend", T.BooleanType()),
    ],
    "dim_product": [
        ("product_key", T.StringType()), ("category", T.StringType()), ("parent_asin", T.StringType()),
        ("title", T.StringType()), ("store", T.StringType()), ("main_category", T.StringType()),
        ("category_path", T.StringType()), ("price", T.DoubleType()), ("average_rating", T.DoubleType()),
        ("rating_number", T.IntegerType()),
    ],
    "fact_review": [
        ("review_id", T.StringType()), ("product_key", T.StringType()), ("date_key", T.IntegerType()),
        ("category", T.StringType()), ("user_id", T.StringType()), ("asin", T.StringType()),
        ("rating", T.ShortType()), ("helpful_vote", T.IntegerType()), ("verified_purchase", T.BooleanType()),
        ("image_count", T.ShortType()), ("has_product", T.BooleanType()), ("review_ts", T.TimestampType()),
        ("review_month", T.StringType()),
    ],
}

# Byte limits of the VARCHAR columns in sql/warehouse/schema.sql. COPY from
# Parquet cannot truncate, so a longer value would fail the load with an
# error about the whole file; checking first names the column instead.
VARCHAR_BYTES = {
    "dim_date": {"month_name": 9, "year_month": 7, "day_name": 9},
    "dim_product": {"product_key": 32, "category": 32, "parent_asin": 16, "title": 4096, "store": 2048,
                    "main_category": 64, "category_path": 512},
    "fact_review": {"review_id": 32, "product_key": 32, "category": 32, "user_id": 64, "asin": 16,
                    "review_month": 7},
}

# Spark names the files it writes part-*. COPY loads every object whose key
# starts with the prefix it is given, so ending the prefix with part- means it
# never sees Spark's _SUCCESS marker or any other file that is not data.
PART_PREFIX = "part-"


def product_key(category, parent_asin):
    """Column: the product's key, MD5 of category|parent_asin.

    Computed the same way for dim_product and fact_review, so the fact table
    gets its key without a lookup, and a review whose product is missing still
    gets the key that product would have.
    """
    return F.md5(F.concat_ws("|", category, parent_asin))


def date_key(date_column):
    """Column: a date as the integer YYYYMMDD."""
    return F.date_format(date_column, "yyyyMMdd").cast("int")


def _typed(df, table):
    return df.select(*[F.col(name).cast(spark_type).alias(name) for name, spark_type in COLUMNS[table]])


def fact_review(enriched):
    """curated reviews_enriched -> rows of dw.fact_review."""
    return _typed(
        enriched
        .withColumn("product_key", product_key(F.col("category"), F.col("parent_asin")))
        .withColumn("date_key", date_key(F.col("review_date"))),
        "fact_review",
    )


def dim_product(products):
    """cleaned products -> rows of dw.dim_product."""
    return _typed(
        products
        .withColumn("product_key", product_key(F.col("category"), F.col("parent_asin")))
        .withColumn("category_path", F.array_join("categories", " > ")),
        "dim_product",
    )


def month_bounds(month):
    """'2023-02' -> (date(2023, 2, 1), date(2023, 2, 28))."""
    year, number = (int(part) for part in month.split("-"))
    return datetime.date(year, number, 1), datetime.date(year, number, calendar.monthrange(year, number)[1])


def dim_date(spark, first_day, last_day):
    """Rows of dw.dim_date for every day from first_day to last_day, inclusive."""
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{first_day.isoformat()}', DATE'{last_day.isoformat()}')) AS full_date"
    )
    d = F.col("full_date")
    return _typed(
        days.select(
            date_key(d).alias("date_key"),
            d,
            F.year(d).alias("year"),
            F.quarter(d).alias("quarter"),
            F.month(d).alias("month"),
            F.date_format(d, "MMMM").alias("month_name"),
            F.date_format(d, "yyyy-MM").alias("year_month"),
            F.dayofmonth(d).alias("day_of_month"),
            # Spark's dayofweek is 1 = Sunday; ISO (and this table) is 1 = Monday.
            (((F.dayofweek(d) + 5) % 7) + 1).alias("day_of_week"),
            F.date_format(d, "EEEE").alias("day_name"),
            F.dayofweek(d).isin(1, 7).alias("is_weekend"),
        ),
        "dim_date",
    )


def oversized(df, table):
    """{column: longest value in bytes} for every column longer than its VARCHAR allows."""
    limits = VARCHAR_BYTES[table]
    longest = df.agg(*[F.max(F.octet_length(c)).alias(c) for c in limits]).first().asDict()
    return {c: longest[c] for c in limits if longest[c] is not None and longest[c] > limits[c]}


# ---------------------------------------------------------------------------
# load SQL
# ---------------------------------------------------------------------------


def _quote(text):
    return "'" + text.replace("'", "''") + "'"


def load_transaction(loads, copy_role_arn):
    """The SQL that replaces warehouse rows with load files, as ONE transaction.

    loads is a list of (table, files_prefix, replace_where): the rows of
    dw.<table> matching replace_where (None = every row) are replaced by the
    files under files_prefix.

    Run with the Data API's BatchExecuteStatement, which runs the list as a
    single transaction: every delete and insert commits together or not at
    all. DELETE, not TRUNCATE: TRUNCATE commits on its own in Redshift.

    LOCK comes first. Up to four months load at once, and two transactions
    that both delete from and insert into fact_review can fail with a
    serializable isolation error. Taking the locks before touching anything
    makes concurrent loads wait their turn instead; each holds them for
    seconds, so waiting costs little.
    """
    targets = [f"{SCHEMA}.{table}" for table, _, _ in loads]
    statements = [f"LOCK {', '.join(targets)}"]
    for (table, files_prefix, replace_where), target in zip(loads, targets):
        stage = f"stage_{table}"
        where = f" WHERE {replace_where}" if replace_where else ""
        statements += [
            f"CREATE TEMP TABLE {stage} (LIKE {target})",
            f"COPY {stage} FROM {_quote(files_prefix + PART_PREFIX)} IAM_ROLE {_quote(copy_role_arn)} FORMAT AS PARQUET",
            f"DELETE FROM {target}{where}",
            f"INSERT INTO {target} SELECT * FROM {stage}",
            f"DROP TABLE {stage}",
        ]
    return statements


def files_prefix(staging_path, table, load_name):
    """Where one load's files for one table go: <staging>/<table>/<month or 'full'>/."""
    return f"{staging_path}/{table}/{load_name}/"


def month_filter(month):
    """The fact_review rows of one month, as a SQL condition."""
    return f"review_month = {_quote(month)}"


def date_filter(first_day, last_day):
    """The dim_date rows from first_day to last_day, as a SQL condition."""
    return f"date_key BETWEEN {first_day:%Y%m%d} AND {last_day:%Y%m%d}"


def check_query(month=None):
    """SQL measuring what the warehouse holds after a load: one row of named numbers.

    With a month, only that month of fact_review is measured.
    """
    where = f"WHERE f.{month_filter(month)}" if month else ""
    return f"""
SELECT
    count(*)                                                 AS fact_rows,
    count(DISTINCT f.review_id)                              AS distinct_review_ids,
    coalesce(sum(f.rating), 0)                               AS rating_sum,
    count(CASE WHEN d.date_key IS NULL THEN 1 END)           AS rows_without_date,
    count(CASE WHEN f.has_product AND p.product_key IS NULL THEN 1 END) AS rows_with_missing_product,
    count(CASE WHEN NOT f.has_product AND p.product_key IS NOT NULL THEN 1 END) AS rows_with_unexpected_product
FROM {SCHEMA}.fact_review f
LEFT JOIN {SCHEMA}.dim_date d ON d.date_key = f.date_key
LEFT JOIN {SCHEMA}.dim_product p ON p.product_key = f.product_key
{where}
""".strip()


def compare(measured, expected):
    """[(name, measured, expected, passed)] for the checks after a load.

    expected holds what Spark wrote: fact_rows and rating_sum.
    """
    rows = [
        ("fact rows = rows exported", measured["fact_rows"], expected["fact_rows"]),
        ("rating sum = rating sum exported", measured["rating_sum"], expected["rating_sum"]),
        ("no duplicate review_id", measured["fact_rows"] - measured["distinct_review_ids"], 0),
        ("every review has its dim_date row", measured["rows_without_date"], 0),
        ("every review with has_product has its dim_product row", measured["rows_with_missing_product"], 0),
        ("no review without has_product has a dim_product row", measured["rows_with_unexpected_product"], 0),
    ]
    return [(name, m, e, m == e) for name, m, e in rows]


# ---------------------------------------------------------------------------
# Redshift Data API
# ---------------------------------------------------------------------------


class StatementFailed(Exception):
    pass


class RedshiftData:
    """Runs SQL through the Redshift Data API and waits for it.

    Authenticates as the caller's own IAM identity (no SecretArn): Redshift
    Serverless issues temporary database credentials for it, and the
    RedshiftDbRoles tag on the caller's IAM role decides its database roles.
    Pass secret_arn to run as the admin user instead (setup only).
    """

    def __init__(self, client, workgroup, database, secret_arn=None, poll_seconds=2, timeout_seconds=1800):
        self.client = client
        self.target = {"WorkgroupName": workgroup, "Database": database}
        if secret_arn:
            self.target["SecretArn"] = secret_arn
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds
        self.last_statement = None  # DescribeStatement's last answer: duration, rows, error

    def _wait(self, statement_id):
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            described = self.last_statement = self.client.describe_statement(Id=statement_id)
            if described["Status"] == "FINISHED":
                return described
            if described["Status"] in ("FAILED", "ABORTED"):
                raise StatementFailed(f"{described['Status']}: {described.get('Error', 'no error message')}")
            if time.monotonic() > deadline:
                self.client.cancel_statement(Id=statement_id)
                raise StatementFailed(f"timed out after {self.timeout_seconds}s; cancelled")
            time.sleep(self.poll_seconds)

    def run(self, statements):
        """Run statements as one transaction. Returns DescribeStatement's result."""
        statement_id = self.client.batch_execute_statement(Sqls=list(statements), **self.target)["Id"]
        return self._wait(statement_id)

    def query(self, sql):
        """Run one SELECT. Returns its rows as dicts."""
        statement_id = self.client.execute_statement(Sql=sql, **self.target)["Id"]
        self._wait(statement_id)
        rows, token = [], None
        while True:
            page = self.client.get_statement_result(Id=statement_id, **({"NextToken": token} if token else {}))
            names = [c["name"] for c in page["ColumnMetadata"]]
            for record in page["Records"]:
                rows.append({n: _field_value(f) for n, f in zip(names, record)})
            token = page.get("NextToken")
            if not token:
                return rows


def _field_value(field):
    """A Data API result field ({'longValue': 3}, {'isNull': True}, ...) -> its value."""
    if field.get("isNull"):
        return None
    return next(iter(field.values()))
