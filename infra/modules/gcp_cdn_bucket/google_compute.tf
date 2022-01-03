# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_backend_bucket" "default" {
  project     = var.project
  name        = "${var.name}-backend"
  bucket_name = google_storage_bucket.default.name
  enable_cdn  = true
}

resource "google_compute_global_address" "default" {
  project    = var.project
  name       = "${var.name}-address"
  ip_version = "IPV4"
}

resource "google_compute_url_map" "default" {
  project         = var.project
  name            = var.name
  default_service = google_compute_backend_bucket.default.self_link
}

resource "google_compute_target_http_proxy" "default" {
  project = var.project
  name    = "${var.name}-http-proxy"
  url_map = google_compute_url_map.default.self_link
}

resource "google_compute_global_forwarding_rule" "http" {
  project    = var.project
  name       = "${var.name}-http"
  target     = google_compute_target_http_proxy.default.self_link
  ip_address = google_compute_global_address.default.address
  port_range = "80"
  depends_on = [google_compute_global_address.default]
}

resource "google_compute_target_https_proxy" "default" {
  project          = var.project
  name             = "${var.name}-https-proxy"
  url_map          = google_compute_url_map.default.self_link
  ssl_certificates = [var.ssl_certificate]
}

resource "google_compute_global_forwarding_rule" "https" {
  project    = var.project
  name       = "${var.name}-https"
  target     = google_compute_target_https_proxy.default.self_link
  ip_address = google_compute_global_address.default.address
  port_range = "443"
  depends_on = [google_compute_global_address.default]
}
