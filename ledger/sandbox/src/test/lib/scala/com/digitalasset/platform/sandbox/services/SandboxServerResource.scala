// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource}
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig

object SandboxServerResource {
  def apply(config: SandboxConfig): Resource[SandboxServer] =
    new OwnedResource(SandboxServer.owner(config))
}
