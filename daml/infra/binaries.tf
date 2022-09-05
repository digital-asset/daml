# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "binaries" {
  project = local.project
  name    = "daml-binaries"
  labels  = local.labels

  # SLA is enough for a cache and is cheaper than MULTI_REGIONAL
  # see https://cloud.google.com/storage/docs/storage-classes
  storage_class = "REGIONAL"

  # Use a normal region since the storage_class is regional
  location = local.region

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_acl" "binaries" {
  bucket      = google_storage_bucket.binaries.name
  default_acl = "publicread"
  role_entity = [
    "OWNER:project-owners-${data.google_project.current.number}",
    "OWNER:project-editors-${data.google_project.current.number}",
    "READER:project-viewers-${data.google_project.current.number}",
    "READER:allUsers",
  ]
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "binaries-ci-create" {
  bucket = google_storage_bucket.binaries.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.writer.email}"
}
resource "google_storage_bucket_iam_member" "binaries-ci-read" {
  bucket = google_storage_bucket.binaries.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.writer.email}"
}


output "binaries_ip" {
  description = "The external IP assigned to the global fowarding rule."
  value       = google_compute_global_address.binaries.address
}

resource "google_compute_backend_bucket" "binaries" {
  project     = local.project
  name        = "binaries-backend"
  bucket_name = google_storage_bucket.binaries.name
  enable_cdn  = true
}

resource "google_compute_global_address" "binaries" {
  project    = local.project
  name       = "binaries-address"
  ip_version = "IPV4"
}

resource "google_compute_url_map" "binaries" {
  project         = local.project
  name            = "binaries"
  default_service = google_compute_backend_bucket.binaries.self_link
}

resource "google_compute_target_http_proxy" "binaries" {
  project = local.project
  name    = "binaries-http-proxy"
  url_map = google_compute_url_map.binaries.self_link
}

resource "google_compute_global_forwarding_rule" "binaries-http" {
  project    = local.project
  name       = "binaries-http"
  target     = google_compute_target_http_proxy.binaries.self_link
  ip_address = google_compute_global_address.binaries.address
  port_range = "80"
  depends_on = [google_compute_global_address.binaries]
}

resource "google_compute_target_https_proxy" "binaries" {
  project          = local.project
  name             = "binaries-https-proxy"
  url_map          = google_compute_url_map.binaries.self_link
  ssl_certificates = ["https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/daml-binaries"]
  ssl_policy       = google_compute_ssl_policy.ssl_policy.self_link
}

resource "google_compute_global_forwarding_rule" "https" {
  project    = local.project
  name       = "binaries-https"
  target     = google_compute_target_https_proxy.binaries.self_link
  ip_address = google_compute_global_address.binaries.address
  port_range = "443"
  depends_on = [google_compute_global_address.binaries]
}
