// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import java.time.Duration

case class BridgeConfig(
    conflictCheckingEnabled: Boolean,
    submissionBufferSize: Int,
    maxDeduplicationDuration: Duration,
)

object BridgeConfig {
  implicit val bridgeConfigReader: ConfigReader[BridgeConfig] = deriveReader[BridgeConfig]
  implicit val bridgeConfigWriter: ConfigWriter[BridgeConfig] = deriveWriter[BridgeConfig]
}
