output "buckets" {
  description = "Bucket name for each data-lake layer."
  value       = { for layer, bucket in aws_s3_bucket.lake : layer => bucket.id }
}

output "raw_bucket" {
  description = "Raw-layer bucket, passed to scripts/download_raw.py."
  value       = aws_s3_bucket.lake["raw"].id
}

output "aws_region" {
  description = "Region the buckets live in, passed to scripts/download_raw.py."
  value       = var.aws_region
}

output "budget_name" {
  value = aws_budgets_budget.project.name
}
