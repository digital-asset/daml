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
import com.typesafe.config.{ConfigFactory, Config => TypesafeConfig}
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File

case class SandboxOnXConfig(
    ledger: Config = Config.Default,
    bridge: BridgeConfig = BridgeConfig.Default,
)
object SandboxOnXConfig {
  import PureConfigReaderWriter.Secure._
  implicit val Convert: ConfigConvert[SandboxOnXConfig] = deriveConvert[SandboxOnXConfig]

  def loadFromConfig(
      configFiles: Seq[File] = Seq(),
      configMap: Map[String, String] = Map(),
      fallback: TypesafeConfig = ConfigFactory.load(),
  ): Either[String, SandboxOnXConfig] = {
    ConfigFactory.invalidateCaches()
    val typesafeConfig = ConfigLoader.toTypesafeConfig(configFiles, configMap, fallback)
    ConfigLoader.loadConfig[SandboxOnXConfig](typesafeConfig)
  }

  def fromLegacy(
      configAdaptor: BridgeConfigAdaptor,
      originalConfig: CliConfig[BridgeConfig],
  ): SandboxOnXConfig = {
    val Unsecure = new PureConfigReaderWriter(false)
    import Unsecure._
    val Convert: ConfigConvert[SandboxOnXConfig] = deriveConvert[SandboxOnXConfig]
    val maxDeduplicationDuration = originalConfig.maxDeduplicationDuration.getOrElse(
      BridgeConfig.DefaultMaximumDeduplicationDuration
    )
    val sandboxOnXConfig = SandboxOnXConfig(
      ledger = LegacyCliConfigConverter.toConfig(configAdaptor, originalConfig),
      bridge = originalConfig.extra.copy(maxDeduplicationDuration = maxDeduplicationDuration),
    )
    // In order to support HOCON configuration via config files and key-value maps -
    // legacy config is rendered without redacting secrets and configuration is applied on top
    val fromConfig = loadFromConfig(
      configFiles = originalConfig.configFiles,
      configMap = originalConfig.configMap,
      fallback = ConfigFactory.parseString(ConfigRenderer.render(sandboxOnXConfig)(Convert)),
    )
    fromConfig.left.foreach { msg =>
      sys.error(s"Failed to parse config after applying config maps and config files: $msg")
    }
    fromConfig.fold(
      msg => sys.error(s"Failed to parse config after applying config maps and config files: $msg"),
      identity,
    )
  }
}
