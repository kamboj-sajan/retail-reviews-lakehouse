# Athena workgroup: where queries run and where their results go.
resource "aws_athena_workgroup" "lake" {
  name        = var.bucket_prefix
  description = "Queries over the ${var.project} data lake."

  # Deletes saved queries and history with the workgroup on teardown.
  force_destroy = true

  configuration {
    # Users of this workgroup cannot override the settings below per query.
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Athena charges per byte scanned. Any single query that would scan more
    # than 20 GiB (about $0.10) is cancelled.
    bytes_scanned_cutoff_per_query = 20 * 1024 * 1024 * 1024

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      # Expired after 7 days by the lifecycle rule in s3.tf.
      output_location = "s3://${aws_s3_bucket.lake["artifacts"].id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
