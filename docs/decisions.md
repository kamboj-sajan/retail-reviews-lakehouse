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

`requirements-dev.txt` pins `pyspark==4.1.1` on Python 3.13 with Java 17, the
versions inside AWS Glue 6.0. The project started on Glue 5.1 (Spark 3.5.6) and
moved to 6.0 at the start of phase 2: in Sydney, 6.0 costs $0.308 per DPU-hour
against $0.44 for earlier versions (AWS Price List API). Spark 4 turns ANSI SQL
mode on by default, which makes invalid casts fail loudly instead of returning
wrong values; every phase 1 test passed unchanged on it.

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

## Phase 2: ETL on AWS Glue

### Glue 6.0 with the FLEX execution class

Glue 6.0 runs Spark 4.1.1 on Python 3.13. Prices in Sydney, from the AWS Price
List API:

| Glue version | Standard | FLEX |
|---|---|---|
| 6.0 and later | $0.308 per DPU-hour | $0.203 per DPU-hour |
| Earlier versions | $0.44 per DPU-hour | $0.29 per DPU-hour |

FLEX runs on spare capacity, so a run can take longer to start. Nothing here
is waiting on a load to the minute, so every job uses it.

### Workers and auto scaling

Each job uses G.1X workers (1 DPU: 4 vCPU, 16 GB) with auto scaling on, so
`number_of_workers` is a ceiling, not a fixed size. That matters for this data
because gzip cannot be split: the read stage of `clean_reviews` has exactly
one task per source file (three in total), however many workers exist.
Without auto scaling, every worker beyond the first would be billed while it
waits for those three tasks. With it, Glue adds workers when the shuffle and
write stages have many tasks queued, and releases them when idle.

### Logic in plain PySpark, not Glue DynamicFrames

The jobs use no Glue-specific API. The same file runs with `python` on a
laptop and as a Glue job, and Glue's own arguments (`--JOB_NAME` and others)
are ignored by `parse_known_args`. `allow_abbrev=False` stops argparse from
treating an unknown Glue argument as an abbreviation of one of ours.

Retries are off in Glue (`max_retries = 0`); the orchestrator in phase 3 owns
retries, so it can see every attempt. Glue job bookmarks are off: idempotency
comes from partition overwrite, which is explicit and covered by tests.

### Least-privilege job role

The Glue job role is written out rather than using the AWS managed
`AWSGlueServiceRole`, which grants access to every bucket named `aws-glue-*`
and more. The role can:

- read the raw bucket;
- read, write and delete in cleaned, curated and quarantine (delete is
  needed because overwriting a partition removes its old files);
- read job code from `artifacts/glue/`, and write only to
  `artifacts/spark-logs/`, `artifacts/glue-temp/` and `artifacts/metrics/`;
- write logs under `/aws-glue/*`.

The trust policy only lets Glue assume the role on behalf of jobs in this
account (`aws:SourceAccount` and `aws:SourceArn` conditions), which protects
against the "confused deputy" problem.

The three Glue log groups are created by Terraform with 14-day retention. If
Glue created them on first run they would keep logs forever and survive
`terraform destroy`.

### Catalog tables defined in Terraform, not by a crawler

A Glue crawler samples files and guesses the schema. It would have to guess
that `review_month` is text rather than a date, it costs DPU time on every run,
and its guess is not reviewed anywhere. The tables in `catalog.tf` are the
schema the jobs write, kept in git next to the code that writes it.

### Partition projection instead of registering partitions

Without projection, every new partition folder has to be added to the catalog
(by a crawler, `MSCK REPAIR TABLE`, or `ALTER TABLE ADD PARTITION`) before
Athena can see it. With projection, the table properties tell Athena that
`category` is one of a fixed list (read from `config/categories.json`) and
`review_month` is any month from 1995-01 to now. A month the pipeline has just
written is queryable at once, and a month with no data simply returns no rows.

Projection is Athena-only: Spark and Redshift Spectrum ignore it. That is
fine here, because the jobs read S3 paths directly and Redshift will load with
`COPY`.

### Athena workgroup with a per-query scan limit

Athena charges $5 per TB scanned in Sydney. The `rrl` workgroup cancels any
query that would scan more than 20 GiB (about $0.10), keeps results for 7
days, and its settings cannot be overridden per query.

### The join to products

`build_curated` LEFT-joins cleaned reviews to cleaned products on
(`category`, `parent_asin`):

- **LEFT, not INNER:** a review whose product is missing from the metadata
  is kept, with `has_product = false`. An inner join would silently remove
  those reviews from every total. The count of such reviews is recorded on
  each run as `reviews_without_product`.
- **On category too:** products are de-duplicated per category. Joining on
  `parent_asin` alone could match a review to the same product listed in two
  categories, and count it twice.
- **Without review text:** `title` and `text` are dropped before the join.
  Parquet stores columns separately, so they are never even read from S3.
- **Aggregates read the joined table back from Parquet** instead of
  recomputing the join three times or caching it in memory.

### Run metrics from the write itself

Every job writes a JSON record per run: rows read, unparseable lines,
duplicates removed, rows written, bytes in and out, and duration. The counts
come from Spark's Observation API, which computes them while the data passes
through the write. Counting separately would mean a second full read of the
input, and for a 6 GB gzip file that cannot be split, that would nearly
double the job's run time. These records are what the phase 7 benchmarks
will report from.

### De-duplication key: (category, user_id, parent_asin, timestamp)

About 1.1% of review lines in the raw files are repeats. The key was chosen by
measuring every candidate over all 64,771,965 raw reviews in Athena.

```
$ python scripts/run_athena_query.py sql/analysis/duplicate_keys.sql
category        unreadable_lines  review_rows  beyond_first_every_field  beyond_first_user_asin_time  beyond_first_user_product_time  beyond_first_user_time  beyond_first_user_product
--------------  ----------------  -----------  ------------------------  ---------------------------  ------------------------------  ----------------------  -------------------------
Electronics                    0     43886944                    477968                       478018                          478035                  478882                     521518
Toys_and_Games                 0     16260406                    174037                       174053                          174056                  174375                     207966
Video_Games                    0      4624615                     53646                        53648                           53653                   53782                      69115
```

"Beyond the first" is the number of rows a de-duplication on that key removes.

```
$ python scripts/run_athena_query.py sql/analysis/duplicate_differences.sql
category        repeated_reviews  extra_copies  most_copies  differ_in_asin  differ_in_rating  differ_in_helpful_vote  largest_vote_difference  differ_in_verified  differ_in_title  differ_in_text
--------------  ----------------  ------------  -----------  --------------  ----------------  ----------------------  -----------------------  ------------------  ---------------  --------------
Electronics               404892        478035           27              17                 2                      49                       19                   3               10              16
Toys_and_Games            146976        174056           15               3                 0                      15                        2                   0                2               2
Video_Games                45201         53653           11               5                 0                       3                        2                   0                1               0

$ python scripts/run_athena_query.py sql/analysis/repeat_reviews.sql
category        user_product_pairs_with_several_times  of_which_identical_text
--------------  -------------------------------------  -----------------------
Electronics                                     40697                     5954
Toys_and_Games                                  29030                     5116
Video_Games                                     12937                     2874
```

What the numbers say:

- **Almost all repeats are exact copies.** Every-field duplicates are within
  0.02% of the chosen key's count in each category. Some reviews appear up to
  27 times.
- **`parent_asin`, not `asin`.** A few reviews (17, 3 and 5) are attached to
  two variants of the same product. Product ratings are reported per
  `parent_asin`, so counting such a review once per variant would inflate
  that product's count.
- **Not every field.** Copies that differ only in `helpful_vote` are
  snapshots of one review taken at different times. The copy with the most
  votes is kept, because vote counts only grow.
- **The product must be in the key.** `(user_id, timestamp)` alone would
  remove 847, 319 and 129 more rows: reviews of *different* products that
  one user submitted in the same millisecond.
- **The timestamp must be in the key.** `(user_id, parent_asin)` would
  remove about 43,000 more rows in Electronics: the same person reviewing
  the same product again later. Some of these repeat identical text (5,954
  pairs in Electronics). They are left as separate reviews. Merging them
  would need a rule for which time to keep, and it would mean looking across
  months, which breaks month-by-month incremental loads.
- **Because the timestamp is in the key, every copy of a review falls in the
  same month.** De-duplicating one month at a time (phase 3) is therefore
  complete; no other month can hold a copy.

Known limitation: in 18 repeated reviews (16 in Electronics, 2 in Toys) the
copies' text differs, and in 2 (Electronics) the rating differs, as if the
review was edited between snapshots. The data has no edit time, so the kept
copy (most votes, then a fixed column order) is not guaranteed to be the
latest edit. That is at most 20 of 597,069 repeated reviews.

The three queries scanned 9.24 GB each and cost $0.046 each
(`benchmarks/athena_queries.jsonl`).

### What the first full run taught: gzip, one task, and FLEX

The Electronics and Toys run of `clean_reviews` took 2,629 seconds and
13,450 DPU-seconds ($0.76). From its driver log:

| UTC | Event |
|---|---|
| 09:07 | Read stage starts: 2 tasks, one per source file. |
| 09:33 | Read stage ends. The 6.47 GB Electronics file took 25 minutes in a single task. |
| 09:34 | The next stage fails with `FetchFailedException`: a worker holding read output was lost. |
| 09:34–09:48 | Spark re-runs the read stage: 14 more minutes. |
| 09:49 | Write stage: 36 tasks of about 30 seconds on 10 workers. |

Two lessons:

1. **A non-splittable file puts a floor under run time.** One task, one core,
   25 minutes, no matter how many workers. More workers would only add idle
   cost; auto scaling at least releases them.
2. **On FLEX, losing a worker can throw away long work.** FLEX uses spare
   capacity, which AWS can take back. Spark recovers by recomputing lost shuffle
   output, but here that meant reading 6.47 GB of gzip again. A long
   single-task stage is the worst case for this.

So phase 3 pays the gzip cost once: a one-time job splits the raw files into
monthly pieces, and every monthly run reads small, separate files.

## Phase 3: incremental monthly loads

### How a month "arrives"

The dataset is static, so arrival is simulated in three steps:

1. **Once:** the Glue job `split_raw_by_month` reads every raw review line
   and writes it, byte for byte, to
   `raw/feed/reviews/category=<c>/review_month=<YYYY-MM>/`. This plays the
   upstream system's monthly exports.
2. **Per month:** `scripts/replay_month.py <month>` copies that month's
   files into `raw/landing/reviews/...`, then writes a marker object
   `raw/landing/_arrived/<month>`.
3. S3 sends the marker's "Object Created" event to EventBridge. A rule that
   matches only keys under `landing/_arrived/` starts the state machine.

The marker is written last, so the load never starts on a half-copied month.
And because there is one marker per month, there is one execution per month,
not one per data file (three categories would otherwise mean three
executions racing to load the same month).

The original downloads in the raw bucket are never modified; `feed/` and
`landing/` sit beside them.

### Why split once instead of reading the raw files each month

A monthly run that read the original files would decompress the whole 6.47 GB
Electronics stream, 25 minutes in one task, to keep about 1/300th of it.
Splitting pays that cost once. After that, a month is about 1/300th of the
data, in three small files.

The split job has no shuffle: each raw file is read by one task, and that task
writes each line straight into its month's file. Spark normally sorts a
task's rows by the partition columns before a partitioned write, which for a
30 GB decompressed stream means spilling to disk. Instead the job keeps one
open writer per month (`spark.sql.maxConcurrentOutputFileWriters`) and turns
off planned writes, which would add the sort back.

It runs on STANDARD, not FLEX. With no shuffle, losing a worker means its
task restarts from byte 0 of a 25-minute read. FLEX is a third cheaper per
DPU-hour; one restart would cost more than that saving.

### The state machine

```
HasMonth? ──no──> MonthFromArrivalKey ──┐
    │yes                                 v
    └───────────────────────────> CheckMonth ──bad──> BadMonth ──┐
                                         │ok                      │
                                         v                        │
                     CleanReviews (Glue .sync, retries) ──fail──> │
                                         v                        v
                     BuildCurated (Glue .sync, retries) ──fail──> NotifyFailure (SNS) ──> LoadFailed
                                         v
                                       Loaded
```

- **Input:** either `{"month": "2023-01"}` (manual runs and the
  idempotency test) or `{"key": "landing/_arrived/2023-01"}` (EventBridge).
  The month is taken from the key with intrinsic functions, so no Lambda is
  needed.
- **`.sync` Glue tasks:** Step Functions starts the Glue run and waits for
  it, so a failed Glue run becomes a failed state that can be retried or
  caught.
- **Two kinds of retry:**
  - `Glue.ConcurrentRunsExceededException`: another month is still using
    the job, which allows one run at a time. Retried every minute, up to 20
    times. This is how two months that arrive together are put in order.
  - `States.TaskFailed`: the Glue run failed. It is retried once after two
    minutes (a lost FLEX worker is the expected cause). A second failure is
    real.
- **Failure notification:** any error is caught and goes to an SNS email
  with the month, the error and the execution ID; the execution then ends
  as failed. The run itself is not hidden by the alert.
- **Logs:** execution history is written to CloudWatch Logs with 14-day
  retention.

Glue's own retries stay off (`max_retries = 0`), so every attempt is visible
in one place, the execution history.

### Why a re-run cannot create duplicates

Three properties together:

1. **Every write replaces whole partitions** (dynamic partition overwrite),
   and every table is partitioned by month. Writing month M replaces month
   M's folders and nothing else.
2. **The run refuses rows from other months.** `clean_reviews --month M`
   drops (and counts as `rows_outside_month`) any row whose timestamp is not
   in M. Otherwise one stray row would make the overwrite replace another
   month with just that row.
3. **De-duplication is complete within a month.** The de-duplication key
   includes the timestamp, so every copy of a review is in the same month.

Running a month twice therefore writes the same files with the same rows.
The test for this exists twice: locally on the sample
(`tests/test_incremental.py`), and on AWS through the real state machine
(`scripts/test_month_idempotency.py`).

### What the replay delivers

The backfill (phase 2) loaded every month. For the replay, the twelve most
recent months, 2022-10 to 2023-09, are deleted from cleaned and curated
(`scripts/reset_months.py`) and delivered again one at a time. That window
includes Black Friday and the December holidays, which the dashboard needs.
After the replay, the per-month counts must equal the backfill's
(`sql/checks/window_counts.sql`, saved before the reset in
`benchmarks/window_counts_backfill.txt`). That checks that loading month by
month gives exactly the same result as the full load.

Products are not replayed: product metadata has no date, and the cleaned
products table from the backfill is used by every monthly run.

### Two things that went wrong in phase 3, and what changed

**`StringMatches` has only one wildcard.** The state machine first checked
the month with `StringMatches = "????-??"`. In Step Functions, `*` is the only
wildcard; `?` is a literal character. So every real month failed the check.
It was caught by delivering a single month before the other eleven, and it cost
no Glue time because the check runs before any Glue task. The check is now a
loose one (a string containing `-`). The exact YYYY-MM check lives in the
Glue jobs' `--month` argument, and a failure there is caught and alerted like
any other.

**A network outage made Terraform plan to destroy live buckets.** While a
plan was refreshing state, DNS on the laptop stopped resolving AWS endpoints.
The AWS provider reported three buckets, all holding data, as "deleted" and
planned 24 creates and 21 destroys. The plan was incomplete, so Terraform
refused to apply it, and the buckets were confirmed intact with `HeadBucket`.
But a plan that was complete *and* wrong would have been applied, because
plan and apply had been run in one command. Now:

- `make tf-apply` saves the plan, runs `scripts/check_plan.py` on it, and
  applies only that saved plan file;
- `check_plan.py` refuses any plan that deletes or replaces a resource
  unless `ALLOW_DESTROY=1` is set.

`make destroy` is still the one intended way to remove everything.

### Per-month cost is mostly fixed overhead

The twelve monthly loads cost between $0.022 and $0.063 each, and the cost
barely follows the data: 2023-09 (5,540 reviews) cost 75% as much as 2023-01
(773,397 reviews). A small Glue run spends most of its billed time starting
Spark and listing S3, on at least two workers. If loads became more frequent
(daily, say), that overhead would dominate. The options would be fewer,
larger loads, or a lighter engine for small months, such as Glue's Python
shell jobs or Athena `INSERT INTO`.

## Phase 4: the Redshift warehouse

Phase 5 was built before phase 4, so the warehouse load could be placed
after the quality gate from the start.

### A star schema: one fact table, two dimensions

`fact_review` holds one row per review; `dim_product` and `dim_date` describe
the product and the day. The curated aggregates already answer the three
dashboard questions, but only at fixed grains; the star answers any grouping
(store by quarter, weekends against weekdays, verified purchases by price
band) without a new Spark job. [warehouse.md](warehouse.md) lists the
columns.

Not built:

- **No `dim_user`.** A user has no attributes in this dataset besides the ID,
  so a user dimension would be a one-column table. `user_id` stays on the
  fact table as a degenerate dimension.
- **No `dim_category`.** Three values, no attributes; it stays a column.
- **Products are loaded only in a full load.** Product metadata arrives
  through `clean_products`, which runs only in a backfill. A monthly load
  adds facts and days, never products.

### Hash keys, not sequence numbers

`product_key` is MD5(`category|parent_asin`), computed in Spark, the same way
`review_id` already is. Kimball-style integer surrogate keys need a lookup
against the dimension during every fact load, and an `IDENTITY` column hands
out different numbers on every reload. A hash needs no lookup, so loads of
different months never coordinate, and a product gets the same key in every
load. A review whose product is missing still gets the key that product
would have, with `has_product = false`.

The cost is width: 32 bytes per key instead of 8, about 1.5 GB more on 64
million rows before Redshift's column compression.

`date_key` is the integer YYYYMMDD: readable, sortable, and computed from the
date itself.

### Load files shaped like the tables

COPY from Parquet fills columns by position and requires the file to have
exactly the table's columns. The lake's files do not: their partition
columns live in folder names. So `load_warehouse` writes its own load files
under `s3://<artifacts>/warehouse/<table>/<month or "full">/`, with the
table's columns in the table's order and types COPY can convert (`ShortType`
for `SMALLINT`). `tests/test_warehouse.py` parses `sql/warehouse/schema.sql`
and fails if the two ever disagree.

Two smaller choices in the same place:

- **Snappy, not zstd.** These files exist only to be read by COPY, and
  snappy is the Parquet codec every reader supports.
- **The COPY prefix ends in `part-`.** COPY loads every object whose key
  starts with the prefix. Ending it with Spark's `part-` file name means
  Spark's `_SUCCESS` marker is never read as data.

COPY cannot truncate a value that is too long for its `VARCHAR` column (the
`TRUNCATECOLUMNS` option does not apply to Parquet). The column sizes are
about twice the longest value found (`sql/analysis/warehouse_column_sizes.sql`:
titles reach 2,000 bytes, stores 915, user IDs 36). The job also measures
every text column's longest value while it writes, and stops before loading,
naming the column, if one would not fit.

### One locked transaction per load

A load is sent as one Data API `BatchExecuteStatement`, which runs its
statements as a single transaction:

```
LOCK dw.dim_date, dw.fact_review
CREATE TEMP TABLE stage_fact_review (LIKE dw.fact_review)
COPY stage_fact_review FROM 's3://.../warehouse/fact_review/2023-01/part-' ... FORMAT AS PARQUET
DELETE FROM dw.fact_review WHERE review_month = '2023-01'
INSERT INTO dw.fact_review SELECT * FROM stage_fact_review
...
```

- **Replace, never append.** Delete the month, insert the month: a re-run
  gives the same rows, as in the lake.
- **DELETE, not TRUNCATE.** TRUNCATE commits on its own in Redshift, which
  would end the transaction halfway through a full load.
- **LOCK first.** Up to four months load at once, and two transactions that
  both delete from and insert into `fact_review` can fail with a
  serializable isolation error. Taking the locks first makes them wait their
  turn instead. Only the Redshift part is serialized, and it takes seconds;
  the Spark work of each month still runs in parallel.

A full load also deletes and re-inserts every row, which leaves deleted rows
behind until Redshift's automatic vacuum reclaims them.

### Checks after the load

Redshift does not enforce primary or foreign keys. After each load the job
queries what Redshift now holds and fails if any of these is false:

- fact rows and rating sum equal what Spark wrote (counted during the write);
- no duplicate `review_id`;
- every fact row has its `dim_date` row;
- every fact row with `has_product` has its `dim_product` row, and none
  without it does.

Redshift's planner *assumes* declared keys hold. So `schema.sql` declares
only the keys these checks prove, and no foreign key on `product_key`, which
a review with a missing product would break.

### Why the load comes after the gate

`LoadWarehouse` is the step after `CheckQuality`. A month that fails the
gate has already been written to the lake (see "The gate stops what comes
after it" below), but it never reaches the warehouse or anything that reads
it. The warehouse only ever holds months that reconciled.

### One Spark job, not a separate loader

The Redshift part of a load is waiting on the Data API. It could be a Glue
Python shell job or Step Functions' own Data API integration, polling
`DescribeStatement` in a loop. Keeping it in the Spark job means one step,
one place where the expected counts live, and the same code tested locally
with a fake Data API. The price is paying for Spark workers while Redshift
runs the load: seconds per month.

### Who can do what

- **`rrl_admin`**: created with the namespace. Redshift generates its password
  and keeps it in Secrets Manager, so it is never in Terraform state. It runs
  `scripts/setup_warehouse.py` once, and ad-hoc queries.
- **The load job**: runs as its own IAM role, `rrl-glue-warehouse`, not the
  lake jobs' role, so lake jobs cannot reach the warehouse and the load job
  cannot write to the lake. It signs in through the Data API with no
  password: Redshift creates the database user `IAMR:rrl-glue-warehouse` and,
  from the role's `RedshiftDbRoles` tag, gives it the database role `loader`,
  which may only select, insert into and delete from the three tables
  (`sql/warehouse/grants.sql`).
- **Redshift itself**: reads load files with `rrl-redshift-copy`, which can
  read `warehouse/` in the artifacts bucket and nothing else.

### Cost controls

Sydney prices (AWS Price List API): $0.419 per RPU-hour, billed per second
while queries run, with a 60-second minimum and nothing while idle;
$0.0261 per GB-month of storage.

- **Base capacity 4 RPUs**, the smallest. 64 million narrow rows is a small
  warehouse.
- **Max capacity 8 RPUs**: Redshift may scale up, but not past twice the base.
- **A usage limit of 24 RPU-hours a month** (about $10) with the action
  "deactivate": past it, Redshift refuses queries, so loads fail and the
  alert goes out. This is the hard limit the budget alarm cannot be (it only
  sends email, hours late).

## Phase 5: data quality

### Two layers of defence: quarantine rules, then a gate

1. **Row rules, while cleaning.** `split_reviews` gives every raw row either
   no problem or one reason, checked in this order: `unparseable_json`,
   `missing_key`, `timestamp_out_of_range`, `rating_not_whole_number`,
   `rating_out_of_range`, and (monthly runs) `outside_month`. Rows with a
   reason are written to the quarantine bucket with that reason and the
   record as JSON text, and counted per reason in the run's metrics. They
   are never loaded and never silently dropped.
2. **Table checks, after curating.** `check_quality` measures the loaded
   month and fails the run if anything is off:
   - reconciliation across layers: landing lines = cleaned + quarantined +
     duplicate copies removed; cleaned = enriched = the `review_count` total
     of each aggregate table;
   - no NULL in key columns;
   - no duplicate `review_id`, and no duplicate review key;
   - every rating from 1 to 5;
   - quarantine rate at most 1% of the month's lines.

The rules make bad rows impossible to load. The checks prove the rules
worked, and catch what rules cannot see: a partial write, a lost file, a
duplicate introduced by a bug.

### Why the quarantine-rate limit

Quarantining a handful of bad rows is the system working. A month where many
rows fail means the source changed or broke, and loading the rows that
passed would publish a partial month that looks complete. 1% is far above
anything the real data produced, and low enough to catch a broken feed.
It is a job argument (`--max_quarantine_rate`), not a constant in code.

### Independent counts, except one

The checks count landing lines, quarantined rows, cleaned rows and curated
totals themselves, from storage, rather than trusting the numbers the
writing jobs reported. The one figure taken from `clean_reviews`' metrics
record is the number of duplicate copies removed: recomputing it would mean
re-running the de-duplication.

### The gate stops what comes after it, not what came before

`check_quality` runs after `build_curated`, so a failing month has already
replaced its cleaned and curated partitions by the time the gate fails. Those
partitions are still correct: bad rows went to quarantine, not into them.
What the gate protects is everything downstream: the Redshift load (phase 4)
will be the step after `CheckQuality`, so a failing month never reaches the
warehouse or the dashboard.

The stricter design is **write-audit-publish**: write the month to a staging
location, check it there, and only then swap it into the lake. It would keep
even the lake untouched by a bad month, at the cost of a second write. With
quarantine already removing bad rows from the lake, gating the warehouse was
enough here.

### Re-runs replace the month's quarantine too

Dynamic partition overwrite only replaces partitions that appear in the new
data. A re-run that finds no bad rows writes no quarantine partition, so the
previous run's quarantine rows would survive and be counted again. Before
writing, `write_quarantine` deletes the partitions the run is responsible
for (`category=*/load_month=<month>` in a monthly run).

### Retry policy for the gate

`CheckQuality` retries only `Glue.ConcurrentRunsExceededException` (the job
is busy). A failed check is deterministic, the same data gives the same
answer, so retrying it would only delay the alert.
