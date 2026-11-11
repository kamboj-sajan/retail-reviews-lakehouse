"""Phase 4: the warehouse load files, the load SQL, and the checks after a load.

There is no Redshift locally, so the Data API is replaced by a fake that
records the SQL it is sent and answers the check query. Everything on the
Spark side runs for real, through the same jobs the pipeline runs.
"""
import datetime
import gzip
import hashlib
import json
import os
import re

import pytest
from pyspark.sql import types as T

import build_curated
import clean_products
import clean_reviews
import load_warehouse
from lakehouse.warehouse import (COLUMNS, VARCHAR_BYTES, RedshiftData, StatementFailed, compare, date_filter,
                                 dim_date, dim_product, fact_review, files_prefix, load_transaction, month_bounds,
                                 month_filter)

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SCHEMA_SQL = os.path.join(ROOT, "sql", "warehouse", "schema.sql")

JAN = 1672531200000  # 2023-01-01 00:00:00 UTC
DAY = 86_400_000
REVIEW = {"user_id": "u1", "asin": "A1", "parent_asin": "P1", "rating": 4.0, "title": "t", "text": "x",
          "timestamp": JAN + 14 * DAY, "helpful_vote": 2, "verified_purchase": True, "images": []}
PRODUCT = {"parent_asin": "P1", "title": "A game", "main_category": "Video Games", "store": "S", "price": "19.99",
           "average_rating": 4.2, "rating_number": 10, "categories": ["Video Games", "PC"], "details": "{}"}


# --- the schema file and the load files agree ------------------------------------

SPARK_TYPE_FOR = {"INTEGER": T.IntegerType(), "SMALLINT": T.ShortType(), "DATE": T.DateType(),
                  "BOOLEAN": T.BooleanType(), "TIMESTAMP": T.TimestampType(), "DOUBLE": T.DoubleType(),
                  "VARCHAR": T.StringType(), "CHAR": T.StringType()}


def ddl_columns():
    """{table: [(column, TYPE, size or None)]} parsed from sql/warehouse/schema.sql."""
    with open(SCHEMA_SQL, encoding="utf-8") as handle:
        text = re.sub(r"--[^\n]*", "", handle.read())
    tables = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS dw\.(\w+) \((.*?)\n\)", text, re.S):
        columns = []
        for line in body.split(","):
            match = re.match(r"\s*(\w+)\s+(INTEGER|SMALLINT|DATE|BOOLEAN|TIMESTAMP|DOUBLE|VARCHAR|CHAR)(?:\((\d+)\))?",
                             line)
            if match and match.group(1) not in ("PRIMARY", "FOREIGN"):
                columns.append((match.group(1), match.group(2), int(match.group(3)) if match.group(3) else None))
        tables[name] = columns
    return tables


def test_load_file_columns_match_the_schema_in_name_order_and_type():
    # COPY from Parquet fills columns by position: any drift here loads
    # values into the wrong columns or fails the load.
    ddl = ddl_columns()
    assert set(ddl) == set(COLUMNS)
    for table, columns in COLUMNS.items():
        assert [c for c, _, _ in ddl[table]] == [c for c, _ in columns], table
        assert [SPARK_TYPE_FOR[t] for _, t, _ in ddl[table]] == [t for _, t in columns], table


def test_varchar_limits_match_the_schema():
    for table, columns in ddl_columns().items():
        assert VARCHAR_BYTES[table] == {c: size for c, t, size in columns if t in ("VARCHAR", "CHAR")}, table


# --- the transforms ---------------------------------------------------------------


def names_and_types(df):
    """What COPY depends on: column names and types, in order (not nullability)."""
    return [(f.name, f.dataType) for f in df.schema.fields]


def test_fact_review_keys(spark):
    enriched = spark.createDataFrame(
        [("r1", "u1", "A1", "P1", 4, 2, True, 0, datetime.datetime(2023, 1, 15, 23, 59),
          datetime.date(2023, 1, 15), True, "Video_Games", "2023-01")],
        "review_id string, user_id string, asin string, parent_asin string, rating int, helpful_vote int, "
        "verified_purchase boolean, image_count int, review_ts timestamp, review_date date, has_product boolean, "
        "category string, review_month string",
    )
    row = fact_review(enriched).first()
    assert row["product_key"] == hashlib.md5(b"Video_Games|P1").hexdigest()
    assert row["date_key"] == 20230115
    assert names_and_types(fact_review(enriched)) == COLUMNS["fact_review"]


def test_dim_date_uses_iso_weekdays_and_covers_every_day(spark):
    first, last = month_bounds("2024-02")
    assert (first, last) == (datetime.date(2024, 2, 1), datetime.date(2024, 2, 29))  # leap year
    days = {r["full_date"]: r for r in dim_date(spark, first, last).collect()}
    assert len(days) == 29
    thursday, sunday = days[datetime.date(2024, 2, 1)], days[datetime.date(2024, 2, 4)]
    assert (thursday["day_of_week"], thursday["day_name"], thursday["is_weekend"]) == (4, "Thursday", False)
    assert (sunday["day_of_week"], sunday["day_name"], sunday["is_weekend"]) == (7, "Sunday", True)
    assert (thursday["date_key"], thursday["year_month"], thursday["quarter"]) == (20240201, "2024-02", 1)


def test_dim_product_joins_the_category_path(spark):
    products = spark.createDataFrame(
        [("P1", "t", "Video Games", "S", 9.5, 4.0, 3, ["Video Games", "PC", "Games"], "{}", "Video_Games")],
        "parent_asin string, title string, main_category string, store string, price double, "
        "average_rating double, rating_number int, categories array<string>, details_json string, category string",
    )
    assert dim_product(products).first()["category_path"] == "Video Games > PC > Games"


# --- the load SQL -----------------------------------------------------------------


def test_monthly_load_is_one_locked_transaction_replacing_only_that_month():
    first, last = month_bounds("2023-01")
    statements = load_transaction(
        [("dim_date", files_prefix("s3://stage", "dim_date", "2023-01"), date_filter(first, last)),
         ("fact_review", files_prefix("s3://stage", "fact_review", "2023-01"), month_filter("2023-01"))],
        "arn:aws:iam::123456789012:role/copy",
    )
    assert statements[0] == "LOCK dw.dim_date, dw.fact_review"
    assert "DELETE FROM dw.dim_date WHERE date_key BETWEEN 20230101 AND 20230131" in statements
    assert "DELETE FROM dw.fact_review WHERE review_month = '2023-01'" in statements
    copies = [s for s in statements if s.startswith("COPY")]
    # Ending the prefix in part- keeps Spark's _SUCCESS file out of the COPY.
    assert "FROM 's3://stage/fact_review/2023-01/part-'" in copies[1]
    assert not any("TRUNCATE" in s for s in statements)  # TRUNCATE would commit on its own


def test_full_load_replaces_whole_tables():
    statements = load_transaction([("dim_product", "s3://stage/dim_product/full/", None)], "role")
    assert "DELETE FROM dw.dim_product" in statements


# --- the Data API client ----------------------------------------------------------


class FakeDataApi:
    """Stands in for boto3's redshift-data client."""

    def __init__(self, check_row=None, fail_with=None, pending_polls=1):
        self.batches, self.queries, self.statuses = [], [], {}
        self.check_row, self.fail_with, self.pending_polls = check_row or {}, fail_with, pending_polls

    def _start(self, kind):
        statement_id = f"{kind}-{len(self.statuses)}"
        self.statuses[statement_id] = ["STARTED"] * self.pending_polls + ["FAILED" if self.fail_with else "FINISHED"]
        return {"Id": statement_id}

    def batch_execute_statement(self, Sqls, WorkgroupName, Database, **_):
        self.batches.append(Sqls)
        return self._start("batch")

    def execute_statement(self, Sql, WorkgroupName, Database, **_):
        self.queries.append(Sql)
        return self._start("query")

    def describe_statement(self, Id):
        status = self.statuses[Id].pop(0) if len(self.statuses[Id]) > 1 else self.statuses[Id][0]
        return {"Id": Id, "Status": status, "Error": self.fail_with, "Duration": 2_500_000_000}

    def get_statement_result(self, Id, NextToken=None):
        names = list(self.check_row)
        return {"ColumnMetadata": [{"name": n} for n in names],
                "Records": [[{"isNull": True} if v is None else {"longValue": v} for v in self.check_row.values()]]}


def test_client_waits_for_the_statement_and_reads_rows():
    api = FakeDataApi(check_row={"fact_rows": 3, "nothing": None}, pending_polls=2)
    redshift = RedshiftData(api, "wg", "reviews", poll_seconds=0)
    assert redshift.run(["SELECT 1"])["Status"] == "FINISHED"
    assert redshift.query("SELECT 1") == [{"fact_rows": 3, "nothing": None}]


def test_client_raises_redshifts_error():
    redshift = RedshiftData(FakeDataApi(fail_with="Serializable isolation violation"), "wg", "reviews", poll_seconds=0)
    with pytest.raises(StatementFailed, match="Serializable isolation violation"):
        redshift.run(["DELETE FROM dw.fact_review"])


def test_compare_flags_each_kind_of_mismatch():
    good = {"fact_rows": 10, "distinct_review_ids": 10, "rating_sum": 40, "rows_without_date": 0,
            "rows_with_missing_product": 0, "rows_with_unexpected_product": 0}
    assert all(passed for *_, passed in compare(good, {"fact_rows": 10, "rating_sum": 40}))
    failed = [name for name, *_, passed in compare(dict(good, distinct_review_ids=9, rows_without_date=1),
                                                   {"fact_rows": 11, "rating_sum": 40}) if not passed]
    assert failed == ["fact rows = rows exported", "no duplicate review_id", "every review has its dim_date row"]


# --- the job, end to end ----------------------------------------------------------


def write_lines(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("\n".join(json.dumps(r) for r in records) + "\n")


@pytest.fixture
def lake(spark, tmp_path):
    """A tiny lake built by the real jobs: three January reviews (one duplicated), one February review."""
    raw, cleaned, curated = (str(tmp_path / d) for d in ("raw", "cleaned", "curated"))
    reviews = [REVIEW, REVIEW, dict(REVIEW, user_id="u2", rating=2.0), dict(REVIEW, user_id="u3", parent_asin="P9"),
               dict(REVIEW, user_id="u4", timestamp=JAN + 40 * DAY)]
    write_lines(f"{raw}/reviews/category=Video_Games/Video_Games.jsonl.gz", reviews)
    write_lines(f"{raw}/meta/category=Video_Games/meta_Video_Games.jsonl.gz", [PRODUCT])
    quarantine = str(tmp_path / "quarantine")
    clean_reviews.main(["--raw_path", raw, "--cleaned_path", cleaned, "--quarantine_path", quarantine])
    clean_products.main(["--raw_path", raw, "--cleaned_path", cleaned, "--quarantine_path", quarantine])
    build_curated.main(["--cleaned_path", cleaned, "--curated_path", curated])
    return {"cleaned": cleaned, "curated": curated, "staging": str(tmp_path / "warehouse"),
            "metrics": str(tmp_path / "metrics")}


def run_job(lake, api, *extra):
    load_warehouse.main(["--cleaned_path", lake["cleaned"], "--curated_path", lake["curated"],
                         "--staging_path", lake["staging"], "--metrics_path", lake["metrics"],
                         "--workgroup", "wg", "--copy_role_arn", "arn:role", *extra], data_client=api)


def redshift_answer(**overrides):
    row = {"fact_rows": 3, "distinct_review_ids": 3, "rating_sum": 10, "rows_without_date": 0,
           "rows_with_missing_product": 0, "rows_with_unexpected_product": 0}
    return dict(row, **overrides)


def test_monthly_load_writes_files_shaped_like_the_tables_and_loads_them(spark, lake):
    # January: u1 (4 stars, sent twice: one copy kept), u2 (2), u3 (4, product P9 not in metadata).
    api = FakeDataApi(check_row=redshift_answer(), pending_polls=0)
    run_job(lake, api, "--month", "2023-01")

    fact = spark.read.parquet(files_prefix(lake["staging"], "fact_review", "2023-01"))
    assert names_and_types(fact) == COLUMNS["fact_review"]
    assert sorted((r["user_id"], r["rating"], r["has_product"]) for r in fact.collect()) == [
        ("u1", 4, True), ("u2", 2, True), ("u3", 4, False)]
    assert spark.read.parquet(files_prefix(lake["staging"], "dim_date", "2023-01")).count() == 31

    [batch] = api.batches
    assert batch[0] == "LOCK dw.dim_date, dw.fact_review"  # no dim_product in a monthly load
    assert "WHERE f.review_month = '2023-01'" in api.queries[0]
    record = spark.read.json(f"{lake['metrics']}/job_name=load_warehouse").first()
    assert (record["fact_review_rows"], record["fact_review_rating_sum"], record["loaded"]) == (3, 10, True)


def test_full_load_includes_products_and_every_day(spark, lake):
    api = FakeDataApi(check_row=redshift_answer(fact_rows=4, distinct_review_ids=4, rating_sum=14), pending_polls=0)
    run_job(lake, api)
    assert api.batches[0][0] == "LOCK dw.dim_date, dw.dim_product, dw.fact_review"
    days = spark.read.parquet(files_prefix(lake["staging"], "dim_date", "full"))
    assert days.count() == 17 + 10  # 2023-01-15 to 2023-02-10
    assert spark.read.parquet(files_prefix(lake["staging"], "dim_product", "full")).count() == 1


def test_a_mismatch_in_redshift_fails_the_run(lake):
    api = FakeDataApi(check_row=redshift_answer(fact_rows=2, distinct_review_ids=2), pending_polls=0)
    with pytest.raises(load_warehouse.WarehouseLoadFailed, match="fact rows = rows exported: measured 2, expected 3"):
        run_job(lake, api, "--month", "2023-01")


def test_a_value_too_long_for_its_column_stops_before_loading(spark, lake, monkeypatch):
    monkeypatch.setitem(VARCHAR_BYTES["fact_review"], "user_id", 1)
    api = FakeDataApi(check_row=redshift_answer(), pending_polls=0)
    with pytest.raises(load_warehouse.WarehouseLoadFailed, match="fact_review.user_id: 2 bytes, column holds 1"):
        run_job(lake, api, "--month", "2023-01")
    assert api.batches == []  # nothing was sent to Redshift


# --- the one-time setup -----------------------------------------------------------


def test_setup_creates_the_schema_and_only_missing_roles():
    import setup_warehouse

    schema = setup_warehouse.statements(os.path.join(ROOT, "sql", "warehouse", "schema.sql"))
    assert len(schema) == 4 and schema[0] == "CREATE SCHEMA IF NOT EXISTS dw"
    assert all("--" not in s for s in schema)  # comments removed before splitting

    for existing, created in ((["loader"], []), ([], ["loader"])):
        api = FakeDataApi(pending_polls=0)
        api.check_row = {"role_name": existing[0]} if existing else {}
        if not existing:
            api.get_statement_result = lambda Id, NextToken=None: {"ColumnMetadata": [{"name": "role_name"}],
                                                                   "Records": []}
        redshift = RedshiftData(api, "wg", "reviews", secret_arn="arn:secret", poll_seconds=0)
        assert setup_warehouse.setup(redshift) == created
        batches = [b[0] for b in api.batches]
        assert ("CREATE ROLE loader" in batches) == bool(created)
        assert api.batches[-1][0] == "GRANT USAGE ON SCHEMA dw TO ROLE loader"
