# Data lake layout

Three layers, one S3 bucket each. A job only ever reads from the layer to its
left and writes to the layer to its right.

```
raw  ──clean_reviews / clean_products──▶  cleaned  ──build_curated──▶  curated
```

| Layer | Format | Who writes it | Rule |
|---|---|---|---|
| raw | gzipped JSON Lines | `scripts/download_raw.py` | Byte-for-byte copy of the source. Never modified. |
| cleaned | Parquet (zstd) | `jobs/clean_reviews.py`, `jobs/clean_products.py` | One row per source record, with real types. No aggregation. |
| curated | Parquet (zstd) | `jobs/build_curated.py` | Aggregated for reporting. Can always be rebuilt from cleaned. |

Locally the same layout lives under `data/sample/raw` (input) and `data/local`
(output), so every job takes the layer roots as arguments and has no idea
whether it is running on a laptop or on Glue.

## raw

```
reviews/category=<category>/<category>.jsonl.gz
meta/category=<category>/meta_<category>.jsonl.gz
_manifests/download-<timestamp>.json
```

The observed fields are in [schema.md](schema.md).

Two more areas in the raw bucket support the monthly replay (phase 3). Both
hold review lines exactly as downloaded, just grouped by month:

```
feed/reviews/category=<category>/review_month=<YYYY-MM>/part-*.txt.gz     written once by split_raw_by_month
landing/reviews/category=<category>/review_month=<YYYY-MM>/part-*.txt.gz  a month copied here = the month has arrived
landing/_arrived/<YYYY-MM>                                                marker written after a month's files; triggers the load
```

## cleaned

### `reviews/`

```
reviews/category=<category>/review_month=<YYYY-MM>/part-*.zstd.parquet
```

One row per review: copies with the same `category`, `user_id`,
`parent_asin` and `timestamp` are reduced to one (docs/decisions.md,
"De-duplication key").

| Column | Type | From | Notes |
|---|---|---|---|
| `review_id` | string | `user_id`, `parent_asin`, `timestamp` | MD5 of the de-duplication key, 32 hex characters. Stable across runs. |
| `user_id` | string | `user_id` | |
| `asin` | string | `asin` | The variant that was reviewed. |
| `parent_asin` | string | `parent_asin` | The product. Join key to `products`. |
| `rating` | int | `rating` | NULL if the source value is not a whole number. |
| `helpful_vote` | int | `helpful_vote` | NULL if the source value does not fit an int. |
| `verified_purchase` | boolean | `verified_purchase` | |
| `title` | string | `title` | |
| `text` | string | `text` | |
| `image_count` | int | `images` | Number of attached images; 0 when there are none. |
| `review_ts` | timestamp | `timestamp` | Source is Unix milliseconds. Stored in UTC. |
| `review_date` | date | `timestamp` | UTC date. |
| `category` | string | folder name | Partition column. |
| `review_month` | string | `timestamp` | Partition column, `YYYY-MM` in UTC. |

A review whose `timestamp` is missing gets a NULL `review_month`, which Spark
writes to a folder named `review_month=__HIVE_DEFAULT_PARTITION__`. The quality
checks in phase 5 will route such rows to quarantine instead.

### `products/`

```
products/category=<category>/part-*.zstd.parquet
```

One row per `category` and `parent_asin`. If a product appears more than
once, the copy with the highest `rating_number` is kept.

| Column | Type | From | Notes |
|---|---|---|---|
| `parent_asin` | string | `parent_asin` | |
| `title` | string | `title` | |
| `main_category` | string | `main_category` | Amazon's own label; can differ from `category`. |
| `store` | string | `store` | |
| `price` | double | `price` | NULL when missing or not a number. |
| `average_rating` | double | `average_rating` | As published by Amazon, not computed here. |
| `rating_number` | int | `rating_number` | As published by Amazon. |
| `categories` | array of string | `categories` | Category path, broadest first. |
| `details_json` | string | `details` | The whole `details` object as JSON text. |
| `category` | string | folder name | Partition column. |

Not carried over from raw: `images`, `videos`, `description`, `features`,
`bought_together`, `author`, `subtitle`.

## curated

### `reviews_enriched/`

```
reviews_enriched/category=<category>/review_month=<YYYY-MM>/part-*.zstd.parquet
```

Every cleaned review, without `title` and `text`, LEFT-joined to its product
on (`category`, `parent_asin`). A review whose product is not in the metadata
is kept with `has_product = false` and NULL product columns.

| Column | Type | Notes |
|---|---|---|
| `review_id` … `review_date` | | As in cleaned `reviews`, minus `title` and `text`. |
| `has_product` | boolean | Whether the product was found in the metadata. |
| `product_title` | string | Product `title`. |
| `store` | string | |
| `price` | double | |
| `main_category` | string | |
| `category`, `review_month` | string | Partition columns. |

### Aggregates

These three tables are built from `reviews_enriched`, are partitioned by
`review_month`, and share these measures:

| Column | Type | Meaning |
|---|---|---|
| `review_count` | bigint | Number of reviews in the group. |
| `rating_sum` | bigint | Sum of ratings in the group. |
| `avg_rating` | double | `rating_sum / review_count`, rounded to 4 places. |
| `verified_count` | bigint | Reviews with `verified_purchase = true`. |

| Table | One row per | Grouping columns | Dashboard visual |
|---|---|---|---|
| `category_monthly/` | category and month | `category`, `review_month` | Rating trends by category |
| `category_daily/` | category and day | `category`, `review_date`, `review_month` | Review volume around sale periods |
| `product_monthly/` | product and month | `category`, `parent_asin`, `product_title`, `store`, `review_month` | Products whose ratings are dropping |

```
category_monthly/review_month=<YYYY-MM>/part-*.zstd.parquet
category_daily/review_month=<YYYY-MM>/part-*.zstd.parquet
product_monthly/review_month=<YYYY-MM>/part-*.zstd.parquet
```

## Catalog

Every table above is registered in the Glue Data Catalog by Terraform
(`infra/terraform/catalog.tf`), in databases `rrl_raw`, `rrl_cleaned` and
`rrl_curated`. Athena finds partitions through partition projection, so a
newly written month is queryable without a crawler or `MSCK REPAIR TABLE`.
`rrl_raw.reviews` and `rrl_raw.products` read the gzipped JSON directly.

## Run metrics

Each job run writes one JSON record to
`<metrics>/job_name=<job>/run_id=<start time>/`: row counts collected during
the write, bytes read and written, and duration. On AWS `<metrics>` is
`s3://<artifacts>/metrics`.
