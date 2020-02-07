// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig

class SandboxServerResource(config: => SandboxConfig) extends Resource[Int] {
  @volatile
  var sandboxServer: SandboxServer = _

  override def value: Int = sandboxServer.port

  override def setup(): Unit = {
    sandboxServer = new SandboxServer(config)
  }

  override def close(): Unit = {
    sandboxServer.close()
    sandboxServer = null
  }
}
