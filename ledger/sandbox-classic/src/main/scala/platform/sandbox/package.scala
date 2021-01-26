// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}

package object sandbox {

  private[sandbox] val Name = LedgerName("Sandbox")

  val DefaultConfig: SandboxConfig = SandboxConfig.defaultConfig.copy(
    seeding = None,
    ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
  )

}
