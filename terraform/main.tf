terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

# GCP Project
provider "google" {
  project = local.envs["project-name"]
  region = local.envs["region"]
  credentials = file("../gcp_credentials.json")  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.envs["project-name"]}-${local.envs["bucket-name"]}" # Concatenating DL bucket & Project name for unique naming
  location      = local.envs["region"]

  storage_class = local.envs["storage-class"]
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
resource "google_bigquery_dataset" "dataset" {
  dataset_id = local.envs["dataset-name"]
  project    = local.envs["project-name"]
  location   = local.envs["region"]
}
