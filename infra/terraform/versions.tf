terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # State is kept in a local file (git-ignored). That is enough for a project
  # with one developer. A team would use an S3 backend with state locking so
  # that two people cannot apply at the same time.
}

provider "aws" {
  region = var.aws_region

  # Safety catch: if the credentials in my shell belong to any other account,
  # Terraform stops before planning instead of building (or destroying) there.
  allowed_account_ids = [var.aws_account_id]

  # Every resource gets these tags, so the project's cost can be filtered in
  # Cost Explorer and nothing is left behind unnoticed after teardown.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}
