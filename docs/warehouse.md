# Warehouse

A star schema in Redshift Serverless (workgroup `rrl-warehouse`, database
`reviews`, schema `dw`), loaded from the lake by `jobs/load_warehouse.py`.
The DDL is [sql/warehouse/schema.sql](../sql/warehouse/schema.sql); the reasons
behind it are in [decisions.md](decisions.md#phase-4-the-redshift-warehouse).

```
                 dim_date                     dim_product
            (one row per day)         (one row per category + product)
                     │                              │
               date_key                       product_key
                     │                              │
                     └──────────  fact_review  ─────┘
                            (one row per review)
```

## Tables

### `dw.fact_review`

One row per cleaned review: `rrl_curated.reviews_enriched` without the
product columns, which live in `dim_product`.

| Column | Type | Notes |
|---|---|---|
| `review_id` | CHAR(32) | As in the lake. Primary key. |
| `product_key` | CHAR(32) | MD5(`category|parent_asin`). Joins to `dim_product`. |
| `date_key` | INTEGER | YYYYMMDD of the review's UTC date. Joins to `dim_date`. |
| `category` | VARCHAR(32) | |
| `user_id` | VARCHAR(64) | Degenerate dimension: users have no other attributes. |
| `asin` | VARCHAR(16) | The variant reviewed. |
| `rating` | SMALLINT | 1 to 5. |
| `helpful_vote` | INTEGER | |
| `verified_purchase` | BOOLEAN | |
| `image_count` | SMALLINT | |
| `has_product` | BOOLEAN | False: the product is not in the metadata, and `product_key` has no `dim_product` row. |
| `review_ts` | TIMESTAMP | UTC. |
| `review_month` | CHAR(7) | `YYYY-MM`. The unit of a monthly load. |

Distributed on `product_key`; compound sort key (`review_month`, `date_key`).

### `dw.dim_product`

One row per product per category, from `rrl_cleaned.products`. Replaced
whole by a full load; monthly loads do not touch it.

| Column | Type | Notes |
|---|---|---|
| `product_key` | CHAR(32) | Primary key. |
| `category`, `parent_asin` | VARCHAR | The natural key. |
| `title`, `store`, `main_category` | VARCHAR | |
| `category_path` | VARCHAR(512) | Amazon's category path, broadest first, joined with ` > `. |
| `price` | DOUBLE PRECISION | NULL when the source has no number. |
| `average_rating`, `rating_number` | | As published by Amazon. |

Distributed on `product_key`, like the fact table, so the join stays on each node.

### `dw.dim_date`

One row per day: every day from the first review to the last after a full
load, and every day of each month a monthly load brings.

| Column | Type | Notes |
|---|---|---|
| `date_key` | INTEGER | YYYYMMDD. Primary key. |
| `full_date` | DATE | |
| `year`, `quarter`, `month`, `day_of_month` | SMALLINT | |
| `month_name`, `day_name` | VARCHAR(9) | `January`, `Monday`. |
| `year_month` | CHAR(7) | Same format as `review_month`. |
| `day_of_week` | SMALLINT | ISO: 1 = Monday ... 7 = Sunday. |
| `is_weekend` | BOOLEAN | Saturday or Sunday. |

Copied to every node (`DISTSTYLE ALL`).

## Loads

| Mode | Started by | Replaces |
|---|---|---|
| Monthly | `LoadWarehouse` in the monthly state machine, after `CheckQuality` | that month of `fact_review`, that month's days in `dim_date` |
| Full | `make glue-run JOB=rrl-load-warehouse` | all of `fact_review`, `dim_date` and `dim_product` |

Load files: `s3://<artifacts>/warehouse/<table>/<YYYY-MM or full>/part-*.snappy.parquet`.
Each run writes a metrics record under `metrics/job_name=load_warehouse/`
with the rows written, the checks run in Redshift and their results.

## Access

| Who | Signs in as | May |
|---|---|---|
| `load_warehouse` | IAM role `rrl-glue-warehouse` → database user `IAMR:rrl-glue-warehouse`, role `loader` | select, insert into and delete from the three tables |
| You | `rrl_admin` (password in Secrets Manager) | everything; used by `make warehouse-setup` and `make redshift-query` |
