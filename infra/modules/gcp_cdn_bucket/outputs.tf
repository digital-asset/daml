# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

output external_ip {
  description = "The external IP assigned to the global fowarding rule."
  value       = google_compute_global_address.default.address
}

output bucket_name {
  description = "Name of the GCS bucket that will receive the objects."
  value       = google_storage_bucket.default.name
}
