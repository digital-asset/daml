# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// Setup the Bazel Bucket + CDN
locals {
  bazel_cache_name = "daml-bazel-cache"

  // see main.tf for additional locals
}

module "bazel_cache" {
  source = "./modules/gcp_cdn_bucket"

  labels               = "${local.labels}"
  name                 = "${local.bazel_cache_name}"
  project              = "${local.project}"
  region               = "${local.region}"
  ssl_certificate      = "${local.ssl_certificate}"
  cache_retention_days = 60
}

// provide a index.html file, so accessing the document root yields something
// nicer than just a 404.
resource "google_storage_bucket_object" "index_bazel_cache" {
  name         = "index.html"
  bucket       = "${module.bazel_cache.bucket_name}"
  content      = "${file("${path.module}/files/index_bazel_cache.html")}"
  content_type = "text/html"
  depends_on   = ["module.nix_cache"]
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "bazel_cache_writer" {
  bucket = "${module.bazel_cache.bucket_name}"

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.writer.email}"
}

output "bazel_cache_ip" {
  value = "${module.bazel_cache.external_ip}"
}
