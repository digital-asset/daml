// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.daml.platform.services.time.TimeProviderType
import com.daml.lf.speedy.Compiler

case class RunnerConfig(
    darPath: Path,
    // If true, we will only list the triggers in the DAR and exit.
    listTriggers: Boolean,
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerParty: Party,
    maxInboundMessageSize: Int,
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeProviderType: Option[TimeProviderType],
    commandTtl: Duration,
    accessTokenFile: Option[Path],
    applicationId: ApplicationId,
    tlsConfig: TlsConfiguration,
    compilerConfig: Compiler.Config,
)

object RunnerConfig {
  private[trigger] val DefaultMaxInboundMessageSize: Int = 4194304
  private[trigger] val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  private[trigger] val DefaultApplicationId: ApplicationId =
    ApplicationId("daml-trigger")
  private[trigger] val DefaultCompilerConfig: Compiler.Config = Compiler.Config.Default

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private val parser = new scopt.OptionParser[RunnerConfig]("trigger-runner") {
    head("trigger-runner")

    opt[String]("dar")
      .required()
      .action((f, c) => c.copy(darPath = Paths.get(f)))
      .text("Path to the dar file containing the trigger")

    opt[String]("trigger-name")
      .action((t, c) => c.copy(triggerIdentifier = t))
      .text("Identifier of the trigger that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[String]("ledger-party")
      .action((t, c) => c.copy(ledgerParty = Party(t)))
      .text("Ledger party")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}"
      )

    opt[Unit]('w', "wall-clock-time")
      .action { (_, c) =>
        setTimeProviderType(c, TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC).")

    opt[Unit]('s', "static-time")
      .action { (_, c) =>
        setTimeProviderType(c, TimeProviderType.Static)
      }
      .text("Use static time.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    opt[String]("access-token-file")
      .action { (f, c) =>
        c.copy(accessTokenFile = Some(Paths.get(f)))
      }
      .text(
        "File from which the access token will be read, required to interact with an authenticated ledger"
      )

    opt[String]("application-id")
      .action { (appId, c) =>
        c.copy(applicationId = ApplicationId(appId))
      }
      .text(s"Application ID used to submit commands. Defaults to ${DefaultApplicationId}")

    opt[Unit]("dev-mode-unsafe")
      .action((_, c) => c.copy(compilerConfig = Compiler.Config.Dev))
      .optional()
      .text(
        "Turns on development mode. Development mode allows development versions of Daml-LF language."
      )
      .hidden()

    TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
      c.copy(tlsConfig = f(c.tlsConfig))
    )

    help("help").text("Print this usage text")

    cmd("list")
      .action((_, c) => c.copy(listTriggers = true))
      .text("List the triggers in the DAR.")

    checkConfig(c =>
      if (c.listTriggers) {
        // I do not want to break the trigger CLI and require a
        // "run" command so I can’t make these options required
        // in general. Therefore, we do this check in checkConfig.
        success
      } else {
        if (c.triggerIdentifier == null) {
          failure("Missing option --trigger-name")
        } else if (c.ledgerHost == null) {
          failure("Missing option --ledger-host")
        } else if (c.ledgerPort == 0) {
          failure("Missing option --ledger-port")
        } else if (c.ledgerParty == null) {
          failure("Missing option --ledger-party")
        } else {
          success
        }
      }
    )
  }

  private def setTimeProviderType(
      config: RunnerConfig,
      timeProviderType: TimeProviderType,
  ): RunnerConfig = {
    if (config.timeProviderType.exists(_ != timeProviderType)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
      )
    }
    config.copy(timeProviderType = Some(timeProviderType))
  }

  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        listTriggers = false,
        triggerIdentifier = null,
        ledgerHost = null,
        ledgerPort = 0,
        ledgerParty = Party(""),
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        timeProviderType = None,
        commandTtl = Duration.ofSeconds(30L),
        accessTokenFile = None,
        tlsConfig = TlsConfiguration(false, None, None, None),
        applicationId = DefaultApplicationId,
        compilerConfig = DefaultCompilerConfig,
      ),
    )
}
