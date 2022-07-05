// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.{
  CliConfig,
  LegacyCliConfigConverter,
  Config,
  ConfigLoader,
  PureConfigReaderWriter,
}
import com.typesafe.config.ConfigFactory
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File

case class SandboxOnXConfig(
    ledger: Config = Config.Default,
    bridge: BridgeConfig = BridgeConfig.Default,
)
object SandboxOnXConfig {
  import PureConfigReaderWriter._
  implicit val Convert: ConfigConvert[SandboxOnXConfig] = deriveConvert[SandboxOnXConfig]

  def loadFromConfig(
      configFiles: Seq[File] = Seq(),
      configMap: Map[String, String] = Map(),
  ): Either[String, SandboxOnXConfig] = {
    ConfigFactory.invalidateCaches()
    val typesafeConfig = ConfigLoader.toTypesafeConfig(configFiles, configMap)
    ConfigLoader.loadConfig[SandboxOnXConfig](typesafeConfig)
  }

  def fromLegacy(
      configAdaptor: BridgeConfigAdaptor,
      originalConfig: CliConfig[BridgeConfig],
  ): SandboxOnXConfig = {
    val maxDeduplicationDuration = originalConfig.maxDeduplicationDuration.getOrElse(
      BridgeConfig.DefaultMaximumDeduplicationDuration
    )
    SandboxOnXConfig(
      ledger = LegacyCliConfigConverter.toConfig(configAdaptor, originalConfig),
      bridge = originalConfig.extra.copy(maxDeduplicationDuration = maxDeduplicationDuration),
    )
  }
}
