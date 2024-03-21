// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.sandbox.BridgeConfig.DefaultMaximumDeduplicationDuration

import java.time.Duration

case class BridgeConfig(
    conflictCheckingEnabled: Boolean = true,
    submissionBufferSize: Int = 500,
    maxDeduplicationDuration: Duration = DefaultMaximumDeduplicationDuration,
)

object BridgeConfig {
  val DefaultMaximumDeduplicationDuration: Duration = Duration.ofMinutes(30L)
  val Default: BridgeConfig = BridgeConfig()
}
