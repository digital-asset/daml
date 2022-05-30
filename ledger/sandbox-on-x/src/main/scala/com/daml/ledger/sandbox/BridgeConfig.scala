// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}
import com.daml.ledger.runner.common.PureConfigReaderWriter._
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

  implicit val bridgeConfigReader: ConfigReader[BridgeConfig] = deriveReader[BridgeConfig]
  implicit val bridgeConfigWriter: ConfigWriter[BridgeConfig] = deriveWriter[BridgeConfig]
}
