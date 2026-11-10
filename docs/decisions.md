# Design decisions

Each entry says what was chosen, what the alternative was, and why. Later
phases add their own sections.

## Phase 0: setup

### Categories: Electronics, Toys_and_Games, Video_Games

Electronics and Toys are the categories where sale periods (Prime Day, Black
Friday, the December holidays) should be most visible, which the dashboard in
phase 6 needs. Video_Games is small, so it is the category every job is run on
first. The category list lives in `config/categories.json`; adding one is a
config change, not a code change.

### Region: ap-southeast-2 (Sydney)

The AWS account this project runs in is restricted to Sydney, so every
resource lives there. The region is one Terraform variable (`aws_region`).

Terraform is also told which account it may touch (`allowed_account_ids`). If
the credentials in the shell belong to a different account, it stops before
planning, so `apply` and `destroy` cannot hit the wrong account by accident.

### Terraform, not AWS CDK

Both would work. Terraform is declarative: the files describe the end state,
`terraform plan` shows the difference before anything is created, and
`terraform destroy` removes everything in one command. CDK would generate
CloudFormation from Python, which adds a bootstrap stack and a second language
to debug when a deployment fails.

State is a local file. That is fine for one developer. A team would keep state
in S3 with locking so two people cannot apply at once.

### One S3 bucket per layer

Layers: `raw`, `cleaned`, `curated`, `quarantine`, plus `artifacts` for job
scripts and query results.

The alternative is one bucket with a folder per layer. Separate buckets make
least-privilege IAM simple: a role that may read raw and write cleaned names
two bucket ARNs, with no prefix conditions. The cost is identical, because S3
charges for bytes and requests, not for buckets.

### Bucket settings

| Setting | Choice | Reason |
|---|---|---|
| Public access | Fully blocked | Nothing here is served publicly. |
| Encryption | SSE-S3 | Free. SSE-KMS would add a KMS request charge to every Spark and Athena read. |
| Versioning | Off | Every layer can be rebuilt from the source files; old versions of multi-GB objects would be billed silently. |
| `force_destroy` | On | Lets `terraform destroy` empty and delete the buckets, which is what makes teardown one command. A production lake would leave it off. |
| Lifecycle | Abort incomplete multipart uploads after 1 day | Interrupted uploads leave invisible parts that are still billed. |
| Bucket policy | Deny non-HTTPS requests | Data is encrypted in transit as well as at rest. |

### Budget alarm

- The budget **excludes credits**. AWS Budgets subtracts credits by default.
  This account pays with free-plan credits, so the default would show zero
  spend and never alert.
- It is **monthly**, because AWS Budgets has no lifetime-total period.
- It is an **alarm, not a brake**. It sends email; it does not stop anything,
  and billing data arrives hours late. The hard limit on idle spend comes in
  phase 4 as a Redshift Serverless usage limit.
- The alert email is a Terraform variable set in a git-ignored file, so no
  address is committed.

### Download: HTTP Range requests into S3 multipart upload

The source files total about 11 GB, more than I want to pass through a laptop
disk, so `scripts/download_raw.py` never writes to disk. It cuts
each file into 64 MB byte ranges; worker threads fetch a range over HTTP and
upload it as one part of an S3 multipart upload.

- **Why ranges, not one long stream:** a network error costs one 64 MB part,
  which is retried, instead of restarting a 6 GB transfer.
- **Idempotent:** each object stores the source file's ETag as metadata. A
  re-run skips any file whose size and source ETag already match.
- **Cleans up:** a failed or interrupted upload is aborted, so no orphaned
  parts are left to bill.
- **Raw stays raw:** the files are stored byte for byte as gzip. They are not
  recompressed or split.

Known consequence for phase 2: gzip cannot be split. Spark has to read each
`.jsonl.gz` file from start to finish in a single task, however many workers
the job has. The first job therefore gets one task per file for the read, and
has to repartition straight afterwards. Phase 2 will measure what this costs.

### Raw key layout

```
s3://<raw>/reviews/category=Electronics/Electronics.jsonl.gz
s3://<raw>/meta/category=Electronics/meta_Electronics.jsonl.gz
```

Review records do not contain their category (see `docs/schema.md`). Writing
it into the path as `category=<name>` means Spark and Athena expose it as a
column without the raw bytes being touched.

## Phase 1: data lake layers

The layout itself (paths, columns, types) is in
[data-lake-layout.md](data-lake-layout.md). This section is the reasoning.

### Plain PySpark, pinned to the Glue runtime

The jobs use the ordinary Spark DataFrame API, not Glue's `DynamicFrame`.
`DynamicFrame` exists to cope with data whose schema is unknown or
inconsistent. Here the schema is declared up front, so it would add nothing,
and it only exists inside Glue, which would make the jobs impossible to run or
test on a laptop.

`requirements-dev.txt` pins `pyspark==3.5.6` on Python 3.11, the versions
inside AWS Glue 5.1. One difference remains: the laptop runs Java 11 and Glue
runs Java 17.

### Logic in functions, jobs as thin wrappers

Everything a job does is a function in `src/lakehouse/` that takes a DataFrame
and returns a DataFrame. The scripts in `jobs/` only parse arguments, read,
call the function and write. That is why the tests can check each rule with
five hand-written rows instead of needing a cluster.

### The schema is declared, not inferred

To infer a JSON schema Spark has to read every file an extra time, which here
means decompressing 11 GB twice. It is also wrong for this data: the product
`details` object has hundreds of distinct keys, and inference would make each
one a column.

Raw fields are read with forgiving types and narrowed afterwards:

- `price` is read as text and converted with `try_cast`, so `"from 12.99"`
  becomes NULL instead of making the whole record unreadable.
- `helpful_vote` is read as a 64-bit number and narrowed with `try_cast`. A
  plain `cast` to int does not fail on overflow in Spark: it wraps around. The
  test value 99,999,999,999 came out as 1,215,752,191 before this was fixed.
- A rating that is not a whole number becomes NULL rather than being rounded.

### Bad lines are flagged, not dropped by the reader

The reader runs in `PERMISSIVE` mode with a `_corrupt_record` column. A line
that is not valid JSON, or has text where a number belongs, is kept with its
original text in that column. For now `clean_reviews` filters those rows out;
phase 5 will write them to the quarantine bucket and count them.

One Spark quirk to remember for phase 5: Spark refuses a query that reads
*only* `_corrupt_record` from a JSON source unless the DataFrame is cached
first.

### Everything is computed in UTC

`spark.sql.session.timeZone` is set to UTC in code. Spark's default is the
machine's time zone. My laptop is on IST and Glue is on UTC, so without this
setting a review written at 23:59 UTC on 31 January would be in the January
partition on Glue and the February partition on my laptop. A test pins this
with exactly that timestamp.

### Partitioning: `category` and `review_month`

- **Why these two columns:** they are the filters the dashboard queries use.
  Athena and Spark skip every folder that a `WHERE` clause rules out, so a
  query for one category and three months reads three folders.
- **Why month, not day:** three categories over roughly 25 years is in the
  order of 900 month folders. By day it would be around 27,000, most of them
  holding a few kilobytes. Every folder costs an S3 listing call and at least
  one file, and many small files are slower to query than a few large ones.
- **Why one `review_month=YYYY-MM` column, not `year=`/`month=`:** an
  incremental run is identified by one value, and a range filter is one
  comparison (`BETWEEN '2022-11' AND '2023-01'`) because the text sorts in
  date order.
- **Month is also the unit of an incremental load** (phase 3): one run writes
  one month folder per category.

### One file per partition folder

Before writing, the data is `repartition`ed on the partition columns. Without
that, each Spark task writes its own file into every folder it has rows for,
which multiplies tasks by months into a flood of tiny files. With it, all rows
for one folder are in one task and it writes one file. `maxRecordsPerFile`
caps a file at one million rows so a very busy month is split.

On the local sample this is visible as a limitation: 60,000 reviews spread
over 665 category-month folders is about 90 rows per file, and Parquet's
per-file overhead dominates. Real months hold thousands of times more rows.

### Re-runs replace, they do not append

`partitionOverwriteMode` is `dynamic` and every write uses `overwrite`. Spark
then replaces exactly the partition folders present in the data being written
and leaves the rest alone. Running the same month twice produces the same
files, not duplicates. Spark's default (`static`) would delete the whole
table on every write.

The alternative is a table format such as Apache Iceberg, which gives
row-level `MERGE` and atomic commits. It would remove the need to align
writes with partitions, at the cost of another moving part to learn and
operate. For a workload that always loads whole months, partition overwrite
is the simplest mechanism that is correct.

### Parquet with zstd compression

Parquet is columnar: a query that needs `rating` and `review_month` never
reads the `text` column from disk. That is the main reason the cleaned layer
is cheaper to query than raw JSON, and phase 7 measures it.

The codec was chosen by measurement on the local sample (60,000 reviews):

```
$ PYTHONPATH=src .venv/bin/python scripts/compare_parquet_codecs.py
rows: 60000
layout       codec           bytes   vs raw gzip JSON
raw JSON     gzip         11534829              1.00x
by month     snappy       21037300              1.82x
by category  snappy       16877122              1.46x
by month     zstd         15214859              1.32x
by category  zstd         10497605              0.91x
by month     gzip         15090918              1.31x
by category  gzip         10782163              0.93x
```

Snappy, Spark's default, is weak on long text, and review text is most of the
bytes. Zstd compresses about as well as gzip and decompresses much faster.
"By month" is inflated on the sample by the tiny files described above; "by
category" is closer to what full-size files will look like. These are sample
numbers only. The real comparison is a phase 7 benchmark.

Redshift does not read the lake's files directly in this design. Its `COPY`
requires files whose columns match the target table exactly, so phase 4
writes its own load files and can use a different codec if needed.

### Timestamps as TIMESTAMP_MICROS

Spark's default Parquet timestamp encoding is INT96, a legacy format. The
session writes the standard `TIMESTAMP_MICROS` instead, which Athena and
Redshift read as an ordinary timestamp.

### Cleaned keeps review text; products are trimmed

Cleaned reviews keep `title` and `text`, so the cleaned layer loses nothing an
analyst could want, and the columnar format means queries that ignore the text
do not pay for it. Cleaned products keep the columns reporting needs and leave
images, videos and long descriptions in raw.

### Curated tables store sums and counts

Each curated table has `review_count` and `rating_sum` next to `avg_rating`.
Counts and sums can be added up again (months into a year, products into a
category). Averages cannot: the average of twelve monthly averages is wrong
unless every month has the same number of reviews.

Curated tables are partitioned by `review_month` for the same reason as the
cleaned reviews: one month can be rebuilt without touching the others. For
the small category tables that means a tiny file per month. That is accepted
in exchange for one idempotency mechanism across every table.
