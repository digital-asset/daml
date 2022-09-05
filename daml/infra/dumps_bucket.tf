# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "dumps" {
  project = local.project
  name    = "daml-dumps"
  labels  = local.labels

  # SLA is enough for a cache and is cheaper than MULTI_REGIONAL
  # see https://cloud.google.com/storage/docs/storage-classes
  storage_class = "REGIONAL"

  # Use a normal region since the storage_class is regional
  location = local.region

  # Enable versioning in case we accidentally delete/overwrite something
  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_acl" "dumps" {
  bucket = google_storage_bucket.dumps.name

  role_entity = [
    "OWNER:project-owners-${data.google_project.current.number}",
    "OWNER:project-editors-${data.google_project.current.number}",
    "READER:project-viewers-${data.google_project.current.number}",
    "READER:allUsers",
  ]

  default_acl = "publicread"
}

# allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "dumps_create" {
  bucket = google_storage_bucket.dumps.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.writer.email}"
}
resource "google_storage_bucket_iam_member" "dumps_read" {
  bucket = google_storage_bucket.dumps.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.writer.email}"
}
