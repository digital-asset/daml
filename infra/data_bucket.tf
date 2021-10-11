# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "data" {
  project = local.project
  name    = "daml-data"
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

resource "google_storage_bucket_acl" "data" {
  bucket = google_storage_bucket.data.name

  role_entity = [
    "OWNER:project-owners-${data.google_project.current.number}",
    "OWNER:project-editors-${data.google_project.current.number}",
    "READER:project-viewers-${data.google_project.current.number}",
  ]
}

// allow rw access for CI writer (see writer.tf)
resource "google_storage_bucket_iam_member" "data_create" {
  bucket = google_storage_bucket.data.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.writer.email}"
}

resource "google_storage_bucket_iam_member" "data_read" {
  bucket = google_storage_bucket.data.name

  # https://cloud.google.com/storage/docs/access-control/iam-roles
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.writer.email}"
}

// allow read access for appr team, as requested by Moritz
locals {
  appr_team = [
    "user:akshay.shirahatti@digitalasset.com",
    "user:andreas.herrmann@digitalasset.com",
    "user:gary.verhaegen@digitalasset.com",
    "user:moritz.kiefer@digitalasset.com",
    "user:stefano.baghino@digitalasset.com",
    "user:stephen.compall@digitalasset.com",
    "user:victor.mueller@digitalasset.com",
  ]
}

resource "google_storage_bucket_iam_member" "appr" {
  for_each = toset(local.appr_team)
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectViewer"
  member   = each.key
}
