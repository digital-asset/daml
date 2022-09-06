// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.logging.ContextualizedLogger
import com.daml.resources.ProgramResource

object CliSandboxOnXRunner {
  // TODO ED: Remove flag once explicit disclosure is deemed stable and all
  //          backing ledgers implement proper validation against malicious clients
  private val UnsafeExplicitDisclosureFlag = "explicit-disclosure-unsafe-enabled"
  private val logger = ContextualizedLogger.get(getClass)
  val RunnerName = "sandbox-on-x"

  def program[T](owner: ResourceOwner[T]): Unit =
    new ProgramResource(owner).run(ResourceContext.apply)

  def run(
      args: collection.Seq[String],
      manipulateConfig: CliConfig[BridgeConfig] => CliConfig[BridgeConfig] = identity,
  ): Unit = {
    val explicitDisclosureEnabled = args.contains(UnsafeExplicitDisclosureFlag)
    val config = CliConfig
      .parse(
        RunnerName,
        BridgeConfig.Parser,
        BridgeConfig.Default,
        args.filterNot(_ == UnsafeExplicitDisclosureFlag),
      )
      .map(manipulateConfig)
      .getOrElse(sys.exit(1))
    runProgram(config, explicitDisclosureEnabled)
  }

  private def runProgram(
      config: CliConfig[BridgeConfig],
      explicitDisclosureUnsafeEnabled: Boolean,
  ): Unit =
    config.mode match {
      case Mode.Run =>
        SandboxOnXConfig
          .loadFromConfig(config.configFiles, config.configMap)
          .fold(
            System.err.println,
            { sandboxOnXConfig =>
              program(
                sox(new BridgeConfigAdaptor, sandboxOnXConfig, explicitDisclosureUnsafeEnabled)
              )
            },
          )
      case Mode.DumpIndexMetadata(jdbcUrls) =>
        program(DumpIndexMetadata(jdbcUrls))
      case Mode.ConvertConfig =>
        Console.out.println(
          ConfigRenderer.render(SandboxOnXConfig.fromLegacy(new BridgeConfigAdaptor, config))
        )
      case Mode.RunLegacyCliConfig =>
        val configAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor
        val sandboxOnXConfig: SandboxOnXConfig = SandboxOnXConfig.fromLegacy(configAdaptor, config)
        program(sox(configAdaptor, sandboxOnXConfig, explicitDisclosureUnsafeEnabled))
    }

  private def sox(
      configAdaptor: BridgeConfigAdaptor,
      sandboxOnXConfig: SandboxOnXConfig,
      explicitDisclosureUnsafeEnabled: Boolean,
  ): ResourceOwner[Unit] = {
    Banner.show(Console.out)
    logger.withoutContext.info(
      "Sandbox-on-X server config: \n" + ConfigRenderer.render(sandboxOnXConfig)
    )
    SandboxOnXRunner
      .owner(
        configAdaptor,
        sandboxOnXConfig.ledger,
        sandboxOnXConfig.bridge,
        explicitDisclosureUnsafeEnabled,
      )
      .map(_ => ())
  }

}
