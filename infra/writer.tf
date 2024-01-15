# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

// This service account will be used to write to the GCS bucket using the
// bazel remote capabilities in the CI

resource "google_service_account" "writer" {
  account_id   = "daml-ci-writer"
  display_name = "CI Writer"
  project      = local.project
}
