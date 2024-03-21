# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// Setup the Nix bucket + CDN
locals {
  nix_cache_name = "daml-nix-cache"

  // see main.tf for additional locals
}

module "nix_cache" {
  source = "./modules/gcp_cdn_bucket"

  labels               = local.labels
  name                 = local.nix_cache_name
  project              = local.project
  region               = local.region
  ssl_certificate      = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/nix-cache"
  ssl_policy           = google_compute_ssl_policy.ssl_policy.self_link
  cache_retention_days = 15
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "nix_cache_writer_create" {
  bucket = module.nix_cache.bucket_name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.writer.email}"
}
resource "google_storage_bucket_iam_member" "nix_cache_writer_read" {
  bucket = module.nix_cache.bucket_name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.writer.email}"
}

output "nix_cache_ip" {
  value = module.nix_cache.external_ip
}
