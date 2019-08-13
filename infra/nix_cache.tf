# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// Setup the Nix bucket + CDN
locals {
  nix_cache_name = "daml-nix-cache"

  // see main.tf for additional locals
}

module "nix_cache" {
  source = "./modules/gcp_cdn_bucket"

  labels               = "${local.labels}"
  name                 = "${local.nix_cache_name}"
  project              = "${local.project}"
  region               = "${local.region}"
  ssl_certificate      = "${local.ssl_certificate}"
  cache_retention_days = 360
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "nix_cache_writer" {
  bucket = "${module.nix_cache.bucket_name}"

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.writer.email}"
}

// provide a index.html file, so accessing the document root yields something
// nicer than just a 404.
resource "google_storage_bucket_object" "index_nix_cache" {
  name         = "index.html"
  bucket       = "${module.nix_cache.bucket_name}"
  content      = "${file("${path.module}/files/index_nix_cache.html")}"
  content_type = "text/html"
  depends_on   = ["module.nix_cache"]
}

// Set ACL for ./index.html
resource "google_storage_object_acl" "index_nix_cache-acl" {
  bucket         = "${module.nix_cache.bucket_name}"
  object         = "${google_storage_bucket_object.index_nix_cache.name}"
  predefined_acl = "publicRead"
}

// provide a nix-cache-info file setting a higher priority
// than cache.nixos.org, so we prefer it
resource "google_storage_bucket_object" "nix-cache-info" {
  name   = "nix-cache-info"
  bucket = "${module.nix_cache.bucket_name}"

  content = <<EOF
StoreDir: /nix/store
WantMassQuery: 1
Priority: 10
  EOF

  content_type = "text/plain"
}

// Set ACL for ./nix-cache-info
resource "google_storage_object_acl" "nix-cache-info-acl" {
  bucket         = "${module.nix_cache.bucket_name}"
  object         = "${google_storage_bucket_object.nix-cache-info.name}"
  predefined_acl = "publicRead"
}

output "nix_cache_ip" {
  value = "${module.nix_cache.external_ip}"
}
