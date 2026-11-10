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

## cleaned

### `reviews/`

```
reviews/category=<category>/review_month=<YYYY-MM>/part-*.zstd.parquet
```

| Column | Type | From | Notes |
|---|---|---|---|
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

All three tables are partitioned by `review_month` and share these measures:

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
| `product_monthly/` | product and month | `category`, `parent_asin`, `review_month` | Products whose ratings are dropping |

```
category_monthly/review_month=<YYYY-MM>/part-*.zstd.parquet
category_daily/review_month=<YYYY-MM>/part-*.zstd.parquet
product_monthly/review_month=<YYYY-MM>/part-*.zstd.parquet
```
