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
  private val logger = ContextualizedLogger.get(getClass)
  val RunnerName = "sandbox-on-x"

  def program[T](owner: ResourceOwner[T]): Unit =
    new ProgramResource(owner).run(ResourceContext.apply)

  def run(
      args: collection.Seq[String],
      manipulateConfig: CliConfig[BridgeConfig] => CliConfig[BridgeConfig] = identity,
  ): Unit = {
    val config = CliConfig
      .parse(
        RunnerName,
        BridgeConfig.Parser,
        BridgeConfig.Default,
        args,
      )
      .map(manipulateConfig)
      .getOrElse(sys.exit(1))
    runProgram(config)
  }

  private def runProgram(
      cliConfig: CliConfig[BridgeConfig]
  ): Unit =
    cliConfig.mode match {
      case Mode.Run =>
        SandboxOnXConfig
          .loadFromConfig(cliConfig.configFiles, cliConfig.configMap)
          .fold(
            System.err.println,
            { sandboxOnXConfig =>
              program(
                sox(new BridgeConfigAdaptor, sandboxOnXConfig)
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
        program(sox(configAdaptor, sandboxOnXConfig))
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
      )
      .map(_ => ())
  }

}
