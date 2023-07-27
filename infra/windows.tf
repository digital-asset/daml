# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  vsts_token   = secret_resource.vsts-token.value
  vsts_account = "digitalasset"
  vsts_pool    = "windows-pool"
  windows = {
    gcp = [
      {
        name       = "ci-w1",
        size       = 0,
        assignment = "default",
        disk_size  = 400,
      },
      {
        name       = "ci-w2"
        size       = 0,
        assignment = "default",
        disk_size  = 400,
      },
    ],
    azure = [
      {
        name       = "dw1",
        size       = 5,
        assignment = "default",
        disk_size  = 400,
      },
      {
        name       = "dw2"
        size       = 0,
        assignment = "default",
        disk_size  = 400,
      },
    ],
  }
}
