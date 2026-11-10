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
      max_workers = 10
    }
    clean_products = {
      description = "raw product JSON -> cleaned/products Parquet"
      max_workers = 4
    }
    build_curated = {
      description = "cleaned reviews + products -> curated tables"
      max_workers = 10
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

  name         = "${var.bucket_prefix}-${replace(each.key, "_", "-")}"
  description  = each.value.description
  role_arn     = aws_iam_role.glue_job.arn
  glue_version = "6.0"

  # FLEX runs on spare capacity: about a third cheaper per DPU-hour, but a run
  # can wait longer to start. Fine for batch loads nobody is waiting on.
  execution_class = "FLEX"

  # G.1X = 1 DPU = 4 vCPU and 16 GB per worker. With auto scaling on,
  # number_of_workers is the MAXIMUM; Glue adds and removes workers based on
  # how many Spark tasks are waiting, and bills only the workers in use.
  worker_type       = "G.1X"
  number_of_workers = each.value.max_workers

  # A run that hangs is billed until it times out, so keep this tight.
  timeout = 60
  # Retries are the orchestrator's job (Step Functions, phase 3). A retry here
  # would be invisible to it.
  max_retries = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts.id}/${aws_s3_object.job_script[each.key].key}"
  }

  default_arguments = {
    "--job-language"   = "python"
    "--extra-py-files" = "s3://${local.artifacts.id}/${aws_s3_object.lakehouse_zip.key}"

    "--raw_path"     = "s3://${aws_s3_bucket.lake["raw"].id}"
    "--cleaned_path" = "s3://${aws_s3_bucket.lake["cleaned"].id}"
    "--curated_path" = "s3://${aws_s3_bucket.lake["curated"].id}"
    # One JSON record per run: row counts, bytes, duration (lakehouse/metrics.py).
    "--metrics_path" = "s3://${local.artifacts.id}/metrics"

    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    # Keeps Spark's event log, so the Spark UI for a finished run can be
    # replayed later to see stages, shuffle sizes and skew.
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.artifacts.id}/spark-logs/"
    "--TempDir"               = "s3://${local.artifacts.id}/glue-temp/"
    # Glue bookmarks track "already processed" files. Idempotency here comes
    # from partition overwrite instead, which is explicit and testable.
    "--job-bookmark-option" = "job-bookmark-disable"
  }

  depends_on = [aws_cloudwatch_log_group.glue, aws_iam_role_policy.glue_job]
}
