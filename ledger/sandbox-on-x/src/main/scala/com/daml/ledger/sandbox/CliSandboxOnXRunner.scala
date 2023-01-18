// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.logging.ContextualizedLogger
import com.daml.resources.ProgramResource

object CliSandboxOnXRunner {
  // TODO ED: Remove flag once explicit disclosure is deemed stable and all
  //          backing ledgers implement proper validation against malicious clients
  //
  // NOTE: The flag is explicitly extracted out of the provided program arguments
  //       and not passed via the HOCON config in order to prevent accidental
  //       enablement, which could render participants vulnerable to malicious clients.
  private val ExplicitDisclosureEnabledArg = "explicit-disclosure-unsafe-enabled"
  private val logger = ContextualizedLogger.get(getClass)
  val RunnerName = "sandbox-on-x"

  def program[T](owner: ResourceOwner[T]): Unit =
    new ProgramResource(owner).run(ResourceContext.apply)

  def run(
      args: collection.Seq[String],
      manipulateConfig: CliConfig[BridgeConfig] => CliConfig[BridgeConfig] = identity,
      registerGlobalOpenTelemetry: Boolean,
  ): Unit = {
    val explicitDisclosureEnabled = args.contains(ExplicitDisclosureEnabledArg)
    val config = CliConfig
      .parse(
        RunnerName,
        BridgeConfig.Parser,
        BridgeConfig.Default,
        args.filterNot(_ == ExplicitDisclosureEnabledArg),
      )
      .map(manipulateConfig)
      .getOrElse(sys.exit(1))
    runProgram(config, explicitDisclosureEnabled, registerGlobalOpenTelemetry)
  }

  private def runProgram(
      cliConfig: CliConfig[BridgeConfig],
      explicitDisclosureUnsafeEnabled: Boolean,
      registerGlobalOpenTelemetry: Boolean,
  ): Unit =
    cliConfig.mode match {
      case Mode.Run =>
        SandboxOnXConfig
          .loadFromConfig(cliConfig.configFiles, cliConfig.configMap)
          .fold(
            System.err.println,
            { sandboxOnXConfig =>
              program(
                sox(
                  new BridgeConfigAdaptor,
                  sandboxOnXConfig,
                  explicitDisclosureUnsafeEnabled,
                  registerGlobalOpenTelemetry,
                )
              )
            },
          )
      case Mode.DumpIndexMetadata(jdbcUrls) =>
        program(DumpIndexMetadata(jdbcUrls))
      case Mode.ConvertConfig =>
        Console.out.println(
          ConfigRenderer.render(SandboxOnXConfig.fromLegacy(new BridgeConfigAdaptor, cliConfig))
        )
      case Mode.RunLegacyCliConfig =>
        val configAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor
        val sandboxOnXConfig: SandboxOnXConfig =
          SandboxOnXConfig.fromLegacy(configAdaptor, cliConfig)
        program(
          sox(
            configAdaptor,
            sandboxOnXConfig,
            explicitDisclosureUnsafeEnabled,
            registerGlobalOpenTelemetry,
          )
        )
      case Mode.PrintDefaultConfig(outputFilePathO) =>
        val text = genDefaultConfigText(cliConfig.configMap)
        outputFilePathO match {
          case Some(outputFilePath) =>
            val _ = Files.write(
              outputFilePath,
              text.getBytes(StandardCharsets.UTF_8),
              StandardOpenOption.CREATE_NEW,
            )
          case None => println(text)
        }
    }

  def genDefaultConfigText(overrides: Map[String, String] = Map()): String = {
    val soxConfig = SandboxOnXConfig.loadFromConfig(configMap = overrides) match {
      case Left(msg) => sys.error(msg)
      case Right(soxConfig) => soxConfig
    }
    val text = ConfigRenderer.render(soxConfig)
    text
  }

  private def sox(
      configAdaptor: BridgeConfigAdaptor,
      sandboxOnXConfig: SandboxOnXConfig,
      explicitDisclosureUnsafeEnabled: Boolean,
      registerGlobalOpenTelemetry: Boolean,
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
        registerGlobalOpenTelemetry,
      )
      .map(_ => ())
  }

}
