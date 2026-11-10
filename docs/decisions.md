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
