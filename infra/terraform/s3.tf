# One bucket per data-lake layer.
#
# Why separate buckets instead of one bucket with raw/, cleaned/, curated/ folders:
# IAM permissions can then be written per bucket. In phase 2 the Glue job role
# gets read-only access to raw and write access to cleaned, and that is a plain
# bucket ARN in the policy, with no prefix conditions to get wrong.
locals {
  buckets = {
    raw        = "Source files exactly as downloaded (gzipped JSON Lines). Never modified."
    cleaned    = "Typed, de-duplicated Parquet, partitioned by category and review month."
    curated    = "Aggregated tables ready for reporting and for COPY into Redshift."
    quarantine = "Rows rejected by the data quality checks."
    artifacts  = "Glue job scripts, Athena query results, benchmark output."
  }
}

# Bucket names must be unique across all of AWS, so add a short random suffix.
resource "random_id" "suffix" {
  byte_length = 3
}

resource "aws_s3_bucket" "lake" {
  for_each = local.buckets

  bucket = "${var.bucket_prefix}-${each.key}-${random_id.suffix.hex}"

  # Lets `terraform destroy` delete a bucket that still has objects in it.
  # That is what makes teardown a single command. It is safe here because
  # everything in these buckets can be rebuilt from the public source files.
  # A production data lake would leave this false.
  force_destroy = true

  tags = {
    Layer = each.key
  }
}

# Nothing in this project is public. Block every way a bucket could become so.
resource "aws_s3_bucket_public_access_block" "lake" {
  for_each = aws_s3_bucket.lake

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Turn ACLs off entirely: access is controlled by IAM policies only.
resource "aws_s3_bucket_ownership_controls" "lake" {
  for_each = aws_s3_bucket.lake

  bucket = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Encrypt at rest with S3-managed keys (SSE-S3). It is free. SSE-KMS would add
# a per-request KMS charge on every Spark and Athena read for no benefit on
# public research data.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = aws_s3_bucket.lake

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning is deliberately left off: every layer can be rebuilt from the
# source files, and old versions of multi-GB objects would be billed silently.

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  for_each = aws_s3_bucket.lake

  bucket = each.value.id

  # The download script and Spark both use multipart uploads. If one is
  # interrupted, its parts are invisible in the console but still billed.
  # This rule deletes such leftovers after a day.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Athena writes a result file for every query. They are only needed briefly.
  dynamic "rule" {
    for_each = each.key == "artifacts" ? [1] : []
    content {
      id     = "expire-athena-results"
      status = "Enabled"
      filter {
        prefix = "athena-results/"
      }
      expiration {
        days = 7
      }
    }
  }
}

# Refuse any request that does not use HTTPS.
data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.lake

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      each.value.arn,
      "${each.value.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.lake

  bucket = each.value.id
  policy = data.aws_iam_policy_document.tls_only[each.key].json

  # S3 can reject a policy change that races with the public access block.
  depends_on = [aws_s3_bucket_public_access_block.lake]
}
