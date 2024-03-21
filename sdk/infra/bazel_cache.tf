# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// Setup the Bazel Bucket + CDN
locals {
  bazel_cache_name = "daml-bazel-cache"

  // see main.tf for additional locals
}

module "bazel_cache" {
  source = "./modules/gcp_cdn_bucket"

  labels               = local.labels
  name                 = local.bazel_cache_name
  project              = local.project
  region               = local.region
  ssl_certificate      = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/bazel-cache"
  ssl_policy           = google_compute_ssl_policy.ssl_policy.self_link
  cache_retention_days = 15
}

// allow rw access for CI writer (see writer.tf)
// Note: it looks like the Bazel cache does not work properly if it does not
// have delete permission, wich is a bit scary.
resource "google_storage_bucket_iam_member" "bazel_cache_writer" {
  bucket = module.bazel_cache.bucket_name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.writer.email}"
}

output "bazel_cache_ip" {
  value = module.bazel_cache.external_ip
}
