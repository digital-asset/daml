// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.{Path, Paths}
import java.io.File
import java.time.Duration

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.{TlsConfiguration, TlsConfigurationCli}

case class RunnerConfig(
    darPath: File,
    scriptIdentifier: String,
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeMode: Option[ScriptTimeMode],
    commandTtl: Duration,
    inputFile: Option[File],
    outputFile: Option[File],
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    jsonApi: Boolean,
    maxInboundMessageSize: Int,
    // While we do have a default application id, we
    // want to differentiate between not specifying the application id
    // and specifying the default for better error messages.
    applicationId: Option[ApplicationId],
)

object RunnerConfig {

  val DefaultTimeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  val DefaultMaxInboundMessageSize: Int = 4194304

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private val parser = new scopt.OptionParser[RunnerConfig]("script-runner") {
    head("script-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the script")

    opt[String]("script-name")
      .required()
      .action((t, c) => c.copy(scriptIdentifier = t))
      .text("Identifier of the script that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .optional()
      .action((t, c) => c.copy(ledgerHost = Some(t)))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .optional()
      .action((t, c) => c.copy(ledgerPort = Some(t)))
      .text("Ledger port")

    opt[File]("participant-config")
      .optional()
      .action((t, c) => c.copy(participantConfig = Some(t)))
      .text("File containing the participant configuration in JSON format")

    opt[Unit]('w', "wall-clock-time")
      .action { (_, c) =>
        setTimeMode(c, ScriptTimeMode.WallClock)
      }
      .text("Use wall clock time (UTC).")

    opt[Unit]('s', "static-time")
      .action { (_, c) =>
        setTimeMode(c, ScriptTimeMode.Static)
      }
      .text("Use static time.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    opt[File]("input-file")
      .action { (t, c) =>
        c.copy(inputFile = Some(t))
      }
      .text("Path to a file containing the input value for the script in JSON format.")

    opt[File]("output-file")
      .action { (t, c) =>
        c.copy(outputFile = Some(t))
      }
      .text("Path to a file where the result of the script will be written to in JSON format.")

    opt[String]("access-token-file")
      .action { (f, c) =>
        c.copy(accessTokenFile = Some(Paths.get(f)))
      }
      .text("File from which the access token will be read, required to interact with an authenticated ledger")

    TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
      c.copy(tlsConfig = f(c.tlsConfig)))

    opt[Unit]("json-api")
      .action { (_, c) =>
        c.copy(jsonApi = true)
      }
      .text("Run DAML Script via the HTTP JSON API instead of via gRPC.")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to $DefaultMaxInboundMessageSize")

    opt[String]("application-id")
      .action((x, c) => c.copy(applicationId = Some(ApplicationId(x))))
      .optional()
      .text(
        s"Application ID used to interact with the ledger. Defaults to ${Runner.DEFAULT_APPLICATION_ID}")

    help("help").text("Print this usage text")

    checkConfig(c => {
      if (c.ledgerHost.isDefined != c.ledgerPort.isDefined) {
        failure("Must specify both --ledger-host and --ledger-port")
      } else if (c.ledgerHost.isDefined && c.participantConfig.isDefined) {
        failure("Cannot specify both --ledger-host and --participant-config")
      } else if (c.ledgerHost.isEmpty && c.participantConfig.isEmpty) {
        failure("Must specify either --ledger-host or --participant-config")
      } else {
        success
      }
    })

  }

  private def setTimeMode(
      config: RunnerConfig,
      timeMode: ScriptTimeMode,
  ): RunnerConfig = {
    if (config.timeMode.exists(_ != timeMode)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous.")
    }
    config.copy(timeMode = Some(timeMode))
  }

  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        scriptIdentifier = null,
        ledgerHost = None,
        ledgerPort = None,
        participantConfig = None,
        timeMode = None,
        commandTtl = Duration.ofSeconds(30L),
        inputFile = None,
        outputFile = None,
        accessTokenFile = None,
        tlsConfig = TlsConfiguration(false, None, None, None),
        jsonApi = false,
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        applicationId = None,
      )
    )
}
