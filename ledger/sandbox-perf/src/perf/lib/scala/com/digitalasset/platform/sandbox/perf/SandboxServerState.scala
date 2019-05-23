// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.{DamlPackageContainer, SandboxConfig}

class SandboxServerState {

  @volatile
  private var _app: SandboxServer = null

  def setup(): Unit = {
    _app = SandboxServer(
      SandboxConfig.default
        .copy(port = 0, damlPackageContainer = DamlPackageContainer(List.empty)))
  }

  def close(): Unit = {
    _app.close()
    _app = null
  }

  def app: SandboxServer = _app

}
