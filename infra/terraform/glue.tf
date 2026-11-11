# Glue jobs: the PySpark code in src/ and jobs/, run on AWS Glue 6.0.

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  repo_root  = "${path.module}/../.."
  artifacts  = aws_s3_bucket.lake["artifacts"]

  # One entry per file in jobs/. The map key is the Glue job's short name.
  glue_jobs = {
    clean_reviews = {
      description = "raw review JSON -> cleaned/reviews Parquet, de-duplicated"
      # Read stage: gzip cannot be split, so each source file is read by a
      # single task. Workers only help after that, in the shuffle and write.
      # Auto scaling (below) releases idle workers during the read.
      max_workers     = 10
      execution_class = "FLEX"
      auto_scaling    = true
      timeout_minutes = 60
      # Up to 4 months can load at the same time. Safe because a monthly run
      # lists and replaces only its own month's folders.
      max_concurrent_runs = 4
    }
    clean_products = {
      description         = "raw product JSON -> cleaned/products Parquet"
      max_workers         = 4
      execution_class     = "FLEX"
      auto_scaling        = true
      timeout_minutes     = 60
      max_concurrent_runs = 1
    }
    build_curated = {
      description         = "cleaned reviews + products -> curated tables"
      max_workers         = 10
      execution_class     = "FLEX"
      auto_scaling        = true
      timeout_minutes     = 60
      max_concurrent_runs = 4
    }
    check_quality = {
      description = "data quality checks for one loaded month; fails the run if any check fails"
      # Counts and a few aggregations over one month of Parquet: small work.
      max_workers         = 4
      execution_class     = "FLEX"
      auto_scaling        = true
      timeout_minutes     = 30
      max_concurrent_runs = 4
    }
    load_warehouse = {
      description = "curated + cleaned -> Redshift star schema; runs after the quality gate"
      # A month is under a million rows; a full load is 64 million rows of
      # load files, where the extra workers help.
      max_workers     = 10
      execution_class = "FLEX"
      auto_scaling    = true
      timeout_minutes = 60
      # Several months may load at once; their Redshift transactions take
      # turns (LOCK in lakehouse/warehouse.py).
      max_concurrent_runs = 4
    }
    split_raw_by_month = {
      description = "run once: raw review files -> one gzip file per category and month (raw/feed)"
      # Three source files means three tasks, and no shuffle, so one executor
      # with 4 cores does all the work: 2 workers = 1 driver + 1 executor.
      max_workers = 2
      # STANDARD, not FLEX: the Electronics task reads one gzip stream for
      # about 25 minutes. If FLEX reclaims that worker, the task starts again
      # from byte 0 (docs/decisions.md). Paying 50% more per DPU-hour is
      # cheaper than paying for that read twice.
      execution_class     = "STANDARD"
      auto_scaling        = false
      timeout_minutes     = 90
      max_concurrent_runs = 1
    }
  }
}

# Arguments only some jobs take.
locals {
  job_arguments = {
    load_warehouse = {
      "--staging_path"  = "s3://${local.artifacts.id}/${local.warehouse_prefix}"
      "--workgroup"     = aws_redshiftserverless_workgroup.warehouse.workgroup_name
      "--database"      = var.redshift_database
      "--copy_role_arn" = aws_iam_role.redshift_copy.arn
      "--region"        = var.aws_region
    }
  }
}

# --- code ------------------------------------------------------------------

# The shared package, zipped so Glue can put it on the Python path.
data "archive_file" "lakehouse" {
  type        = "zip"
  source_dir  = "${local.repo_root}/src"
  output_path = "${path.module}/build/lakehouse.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc"]
}

resource "aws_s3_object" "lakehouse_zip" {
  bucket = local.artifacts.id
  key    = "glue/lakehouse.zip"
  source = data.archive_file.lakehouse.output_path
  # Changing the code changes the hash, which makes Terraform upload again.
  etag = data.archive_file.lakehouse.output_md5
}

resource "aws_s3_object" "job_script" {
  for_each = local.glue_jobs

  bucket = local.artifacts.id
  key    = "glue/jobs/${each.key}.py"
  source = "${local.repo_root}/jobs/${each.key}.py"
  etag   = filemd5("${local.repo_root}/jobs/${each.key}.py")
}

# --- logs ------------------------------------------------------------------

# Glue writes to these log groups. If Glue creates them itself they keep logs
# forever, which is billed forever and survives `terraform destroy`. Creating
# them here sets a retention period and puts them under teardown.
resource "aws_cloudwatch_log_group" "glue" {
  for_each = toset(["/aws-glue/jobs/logs-v2", "/aws-glue/jobs/output", "/aws-glue/jobs/error"])

  name              = each.value
  retention_in_days = 14
}

# --- IAM role the jobs run as ------------------------------------------------

data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    # Confused-deputy protection: only Glue jobs in THIS account may use the
    # role, not Glue acting on behalf of some other account.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:glue:${var.aws_region}:${local.account_id}:job/*"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.bucket_prefix}-glue-job"
  description        = "Runs the ${var.project} Glue jobs."
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

# Least privilege, written out instead of attaching AWSGlueServiceRole, which
# grants S3 access to every bucket named aws-glue-* and more besides.
data "aws_iam_policy_document" "glue_job" {
  statement {
    sid     = "ListLakeBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [
      aws_s3_bucket.lake["raw"].arn,
      aws_s3_bucket.lake["cleaned"].arn,
      aws_s3_bucket.lake["curated"].arn,
      aws_s3_bucket.lake["quarantine"].arn,
      local.artifacts.arn,
    ]
  }

  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake["raw"].arn}/*"]
  }

  # The one-time monthly split writes under raw/feed/. Everything else in raw
  # (the downloaded files, landing/) stays read-only to Glue.
  statement {
    sid = "WriteRawFeed"
    actions = [
      "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lake["raw"].arn}/feed/*"]
  }

  # Delete is needed because overwriting a partition deletes its old files.
  statement {
    sid = "ReadWriteCleanedCuratedQuarantine"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${aws_s3_bucket.lake["cleaned"].arn}/*",
      "${aws_s3_bucket.lake["curated"].arn}/*",
      "${aws_s3_bucket.lake["quarantine"].arn}/*",
    ]
  }

  statement {
    sid       = "ReadJobCode"
    actions   = ["s3:GetObject"]
    resources = ["${local.artifacts.arn}/glue/*"]
  }

  statement {
    sid = "WriteSparkLogsTempAndMetrics"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${local.artifacts.arn}/spark-logs/*",
      "${local.artifacts.arn}/glue-temp/*",
      "${local.artifacts.arn}/metrics/*",
    ]
  }

  statement {
    sid       = "WriteGlueLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "lake-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# --- the jobs ----------------------------------------------------------------

resource "aws_glue_job" "job" {
  for_each = local.glue_jobs

  name        = "${var.bucket_prefix}-${replace(each.key, "_", "-")}"
  description = each.value.description
  # The warehouse load runs as its own role (warehouse.tf); every other job as the lake role.
  role_arn     = contains(local.warehouse_jobs, each.key) ? aws_iam_role.glue_warehouse.arn : aws_iam_role.glue_job.arn
  glue_version = "6.0"

  # FLEX runs on spare capacity: about a third cheaper per DPU-hour, but a run
  # can wait longer to start and a worker can be taken back mid-run. Fine for
  # batch loads nobody is waiting on; see split_raw_by_month for the exception.
  execution_class = each.value.execution_class

  # G.1X = 1 DPU = 4 vCPU and 16 GB per worker. With auto scaling on,
  # number_of_workers is the MAXIMUM; Glue adds and removes workers based on
  # how many Spark tasks are waiting, and bills only the workers in use.
  worker_type       = "G.1X"
  number_of_workers = each.value.max_workers

  # A run that hangs is billed until it times out, so keep this tight.
  timeout = each.value.timeout_minutes

  execution_property {
    max_concurrent_runs = each.value.max_concurrent_runs
  }
  # Retries are the orchestrator's job (Step Functions, phase 3). A retry here
  # would be invisible to it.
  max_retries = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts.id}/${aws_s3_object.job_script[each.key].key}"
  }

  default_arguments = merge({
    "--job-language"   = "python"
    "--extra-py-files" = "s3://${local.artifacts.id}/${aws_s3_object.lakehouse_zip.key}"

    "--raw_path"        = "s3://${aws_s3_bucket.lake["raw"].id}"
    "--cleaned_path"    = "s3://${aws_s3_bucket.lake["cleaned"].id}"
    "--curated_path"    = "s3://${aws_s3_bucket.lake["curated"].id}"
    "--quarantine_path" = "s3://${aws_s3_bucket.lake["quarantine"].id}"
    # Read by check_quality only; the other jobs ignore it.
    "--max_quarantine_rate" = "0.01"
    # One JSON record per run: row counts, bytes, duration (lakehouse/metrics.py).
    "--metrics_path" = "s3://${local.artifacts.id}/metrics"

    "--enable-auto-scaling"              = tostring(each.value.auto_scaling)
    "--enable-continuous-cloudwatch-log" = "true"
    # Keeps Spark's event log, so the Spark UI for a finished run can be
    # replayed later to see stages, shuffle sizes and skew.
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.artifacts.id}/spark-logs/"
    "--TempDir"               = "s3://${local.artifacts.id}/glue-temp/"
    # Glue bookmarks track "already processed" files. Idempotency here comes
    # from partition overwrite instead, which is explicit and testable.
    "--job-bookmark-option" = "job-bookmark-disable"
  }, lookup(local.job_arguments, each.key, {}))

  depends_on = [aws_cloudwatch_log_group.glue, aws_iam_role_policy.glue_job, aws_iam_role_policy.glue_warehouse]
}
