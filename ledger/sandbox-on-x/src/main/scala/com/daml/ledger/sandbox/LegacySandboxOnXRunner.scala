// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.SandboxOnXRunner.run

object LegacySandboxOnXRunner {
  val RunnerName = "sandbox-on-x"

  def owner(
      args: collection.Seq[String],
      manipulateConfig: LegacyCliConfig[BridgeConfig] => LegacyCliConfig[BridgeConfig] = identity,
      configProvider: ConfigProvider[BridgeConfig] = new BridgeConfigProvider,
  ): ResourceOwner[Unit] =
    LegacyCliConfig
      .owner(
        RunnerName,
        configProvider.extraConfigParser,
        configProvider.defaultExtraConfig,
        args,
      )
      .map(manipulateConfig)
      .flatMap(owner(configProvider))

  def owner(
      configProvider: ConfigProvider[BridgeConfig]
  )(originalConfig: LegacyCliConfig[BridgeConfig]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        originalConfig.mode match {
          case Mode.DumpIndexMetadata(jdbcUrls) =>
            DumpIndexMetadata(jdbcUrls, RunnerName)
            sys.exit(0)
          case Mode.Run =>
            val config = configProvider.fromLegacyCliConfig(originalConfig)
            run(configProvider, config, originalConfig.extra)
        }
      }
    }
}
