# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "data" {
  project = "${local.project}"
  name    = "daml-data"
  labels  = "${local.labels}"

  # SLA is enough for a cache and is cheaper than MULTI_REGIONAL
  # see https://cloud.google.com/storage/docs/storage-classes
  storage_class = "REGIONAL"

  # Use a normal region since the storage_class is regional
  location = "${local.region}"
}

resource "google_storage_bucket_acl" "data" {
  bucket = "${google_storage_bucket.data.name}"

  role_entity = [
    "OWNER:project-owners-${data.google_project.current.number}",
    "OWNER:project-editors-${data.google_project.current.number}",
    "READER:project-viewers-${data.google_project.current.number}",
  ]
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "data" {
  bucket = "${google_storage_bucket.data.name}"

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.writer.email}"
}
