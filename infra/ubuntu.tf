# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  ubuntu = {
    azure = [
      {
        name       = "du1",
        disk_size  = 400,
        size       = 10,
        assignment = "default",
        nix        = "su --command \"sh <(curl -sSfL https://releases.nixos.org/nix/nix-2.17.0/install) --daemon\" --login vsts"
      },
      {
        name       = "du2",
        disk_size  = 400,
        size       = 0,
        assignment = "default",
        nix        = "su --command \"sh <(curl -sSfL https://releases.nixos.org/nix/nix-2.17.0/install) --daemon\" --login vsts"
      },
    ]
  }
}
