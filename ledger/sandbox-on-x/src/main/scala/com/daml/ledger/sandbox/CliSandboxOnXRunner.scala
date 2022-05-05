// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.SandboxOnXRunner.run

object CliSandboxOnXRunner {
  val RunnerName = "sandbox-on-x"

  def owner(
      args: collection.Seq[String],
      manipulateConfig: CliConfig[BridgeConfig] => CliConfig[BridgeConfig] = identity,
  ): ResourceOwner[Unit] = {
    val configProvider = new BridgeCliConfigProvider
    CliConfig
      .owner(
        RunnerName,
        configProvider.extraConfigParser,
        configProvider.defaultExtraConfig,
        args,
      )
      .map(manipulateConfig)
      .flatMap(owner)
  }

  def owner(originalConfig: CliConfig[BridgeConfig]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        val configAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor
        originalConfig.mode match {
          case Mode.DumpIndexMetadata(jdbcUrls) =>
            DumpIndexMetadata(jdbcUrls, RunnerName)
            sys.exit(0)
          case Mode.Run =>
            val config = CliConfigConverter.toConfig(configAdaptor, originalConfig)
            run(
              configAdaptor,
              config,
              originalConfig.extra.copy(maxDeduplicationDuration =
                originalConfig.maxDeduplicationDuration.getOrElse(
                  BridgeConfig.DefaultMaximumDeduplicationDuration
                )
              ),
            )
        }
      }
    }
}
