// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import java.time.Duration

import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}

package object sandbox {

  private[sandbox] val Name = LedgerName("Sandbox")

  val DefaultConfig: SandboxConfig = SandboxConfig.defaultConfig.copy(
    seeding = None,
    delayBeforeSubmittingLedgerConfiguration = Duration.ZERO,
  )

}
