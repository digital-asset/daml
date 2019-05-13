# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// Setup the documentation bucket
locals {
  doc_bucket = "daml-docs"

  // see main.tf for additional locals
}

module "daml_docs" {
  source = "./modules/gcp_cdn_bucket"

  labels          = "${local.labels}"
  name            = "${local.doc_bucket}"
  project         = "${local.project}"
  region          = "${local.region}"
  ssl_certificate = "${local.ssl_certificate}"

  // We do not want to delete anything here, but Terraform composition is hard
  // so instead keep objects for 100 years.
  cache_retention_days = 36500
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "docs_bucket_writer" {
  bucket = "${module.daml_docs.bucket_name}"

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.writer.email}"
}

output "daml_docs_ip" {
  value = "${module.daml_docs.external_ip}"
}
