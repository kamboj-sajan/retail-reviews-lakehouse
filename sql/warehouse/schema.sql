-- The warehouse: a star schema in Redshift Serverless.
--
-- Run once, as the admin user, by scripts/setup_warehouse.py. Every statement
-- can be run again safely. The column ORDER of each table matters: COPY from
-- Parquet fills columns by position, so jobs/load_warehouse.py writes its load
-- files with exactly these columns in exactly this order, and
-- tests/test_warehouse.py fails if the two drift apart.
--
-- Text sizes are in bytes (Redshift VARCHAR(n) counts bytes, not characters)
-- and are about twice the longest value in the data
-- (sql/analysis/warehouse_column_sizes.sql). COPY from Parquet cannot truncate,
-- so a longer value would fail the load; load_warehouse checks first and names
-- the column instead.
--
-- Keys are declared for the query planner and for documentation. Redshift
-- does not enforce them, but it assumes they hold, so only keys that always
-- hold are declared, and load_warehouse's checks prove they do.

CREATE SCHEMA IF NOT EXISTS dw;

-- One row per calendar day. Small, so a copy is kept on every node (ALL).
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      INTEGER     NOT NULL,  -- YYYYMMDD, e.g. 20230115
    full_date     DATE        NOT NULL,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(9)  NOT NULL,
    year_month    CHAR(7)     NOT NULL,  -- 2023-01, same as review_month
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,  -- 1 = Monday ... 7 = Sunday (ISO)
    day_name      VARCHAR(9)  NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- One row per product per category, from the cleaned product metadata.
-- product_key is MD5(category|parent_asin): computed the same way in Spark
-- for the fact table, so no lookup is needed to assign it.
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     CHAR(32)          NOT NULL,
    category        VARCHAR(32)       NOT NULL,
    parent_asin     VARCHAR(16)       NOT NULL,
    title           VARCHAR(4096),
    store           VARCHAR(2048),
    main_category   VARCHAR(64),
    category_path   VARCHAR(512),              -- Amazon's category path, broadest first, joined with ' > '
    price           DOUBLE PRECISION,
    average_rating  DOUBLE PRECISION,          -- as published by Amazon
    rating_number   INTEGER,                   -- as published by Amazon
    PRIMARY KEY (product_key)
)
DISTSTYLE KEY
DISTKEY (product_key)
SORTKEY (category, parent_asin);

-- One row per review. Distributed on product_key, like dim_product, so the
-- product join happens on each node without moving rows between nodes.
-- Sorted by month then day: a monthly load deletes and re-inserts one
-- review_month, and dashboard queries filter on date ranges; both read only
-- the blocks they need.
CREATE TABLE IF NOT EXISTS dw.fact_review (
    review_id          CHAR(32)     NOT NULL,
    product_key        CHAR(32)     NOT NULL,
    date_key           INTEGER      NOT NULL,
    category           VARCHAR(32)  NOT NULL,
    user_id            VARCHAR(64)  NOT NULL,
    asin               VARCHAR(16),
    rating             SMALLINT     NOT NULL,
    helpful_vote       INTEGER,
    verified_purchase  BOOLEAN,
    image_count        SMALLINT     NOT NULL,
    has_product        BOOLEAN      NOT NULL,  -- false: product_key has no row in dim_product
    review_ts          TIMESTAMP    NOT NULL,  -- UTC
    review_month       CHAR(7)      NOT NULL,
    PRIMARY KEY (review_id),
    -- No foreign key on product_key: a review whose product is missing from
    -- the metadata (has_product = false) has no dim_product row, and the
    -- planner trusts declared keys, so declaring one would be a lie it acts on.
    FOREIGN KEY (date_key) REFERENCES dw.dim_date (date_key)
)
DISTSTYLE KEY
DISTKEY (product_key)
COMPOUND SORTKEY (review_month, date_key);
