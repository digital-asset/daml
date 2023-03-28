# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  ubuntu = {
    gcp = [
      {
        name       = "ci-u1",
        disk_size  = 400,
        size       = 30,
        assignment = "default",
      },
      {
        name       = "ci-u2",
        disk_size  = 400,
        size       = 0,
        assignment = "default",
      },
    ],
    azure = [
      {
        name       = "ubuntu",
        disk_size  = 400,
        size       = 1,
        assignment = "azure",
      },
    ]
  }
}
