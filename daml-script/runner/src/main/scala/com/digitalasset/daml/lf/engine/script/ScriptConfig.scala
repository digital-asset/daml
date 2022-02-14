// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.{Path, Paths}
import java.io.File

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.{TlsConfiguration, TlsConfigurationCli}
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode

sealed abstract class CliMode extends Product with Serializable
object CliMode {
  // Run a single script in the DAR
  final case class RunOne(scriptId: String) extends CliMode
  // Run all scripts in the DAR
  final case object RunAll extends CliMode
}

sealed abstract class ScriptCommand extends Product with Serializable
object ScriptCommand {
  final case class RunOne(conf: RunnerConfig) extends ScriptCommand
  final case class RunAll(conf: TestConfig) extends ScriptCommand
}

case class ScriptConfig(
    darPath: File,
    mode: Option[CliMode],
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeMode: Option[ScriptTimeMode],
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
) {
  def toCommand(): Either[String, ScriptCommand] =
    for {
      mode <- mode.toRight("Either --script-name or --all must be specified")
      resolvedTimeMode = timeMode.getOrElse(ScriptConfig.DefaultTimeMode)
      conf <- mode match {
        case CliMode.RunOne(id) =>
          Right(
            ScriptCommand.RunOne(
              RunnerConfig(
                darPath = darPath,
                ledgerHost = ledgerHost,
                ledgerPort = ledgerPort,
                participantConfig = participantConfig,
                timeMode = resolvedTimeMode,
                inputFile = inputFile,
                outputFile = outputFile,
                accessTokenFile = accessTokenFile,
                tlsConfig = tlsConfig,
                jsonApi = jsonApi,
                maxInboundMessageSize = maxInboundMessageSize,
                applicationId = applicationId,
                scriptIdentifier = id,
              )
            )
          )
        case CliMode.RunAll =>
          def incompatible[A](name: String, isValid: Boolean): Either[String, Unit] =
            Either.cond(isValid, (), s"--${name} is incompatible with --all")
          for {
            _ <- incompatible("input-file", inputFile.isEmpty)
            _ <- incompatible("output-file", outputFile.isEmpty)
            _ <- incompatible("application-id", applicationId.isEmpty)
            _ <- incompatible("json-api", !jsonApi)
            _ <- Either.cond(outputFile.isEmpty, (), "--output-file is incompatible with --all")
          } yield ScriptCommand.RunAll(
            TestConfig(
              darPath = darPath,
              ledgerHost = ledgerHost,
              ledgerPort = ledgerPort,
              participantConfig = participantConfig,
              timeMode = resolvedTimeMode,
              maxInboundMessageSize = maxInboundMessageSize,
            )
          )
      }
    } yield conf
}

object ScriptConfig {

  val DefaultTimeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  val DefaultMaxInboundMessageSize: Int = 4194304

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private val parser = new scopt.OptionParser[ScriptConfig]("script-runner") {
    head("script-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the script")

    opt[String]("script-name")
      .optional()
      .action((t, c) => setMode(c, CliMode.RunOne(t)))
      .text("Identifier of the script that should be run in the format Module.Name:Entity.Name")

    opt[Unit]("all")
      .optional()
      .action((_, c) => setMode(c, CliMode.RunAll))
      .text("Run all scripts in the main DALF in the DAR")

    opt[String]("ledger-host")
      .optional()
      .action((t, c) => c.copy(ledgerHost = Some(t)))
      .text(
        "Ledger hostname. If --json-api is specified, this is used to connect to the JSON API and must include the protocol, e.g. \"http://localhost\"."
      )

    opt[Int]("ledger-port")
      .optional()
      .action((t, c) => c.copy(ledgerPort = Some(t)))
      .text(
        "Ledger port. If --json-api is specified, this is the port used to connect to the JSON API."
      )

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
      .text(
        "File from which the access token will be read, required to interact with an authenticated ledger or the JSON API."
      )

    TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
      c.copy(tlsConfig = f(c.tlsConfig))
    )

    opt[Unit]("json-api")
      .action { (_, c) =>
        c.copy(jsonApi = true)
      }
      .text(
        "Run Daml Script via the HTTP JSON API instead of via gRPC; use --ledger-host and --ledger-port for JSON API host and port. The JSON API requires an access token."
      )

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to $DefaultMaxInboundMessageSize"
      )

    opt[String]("application-id")
      .action((x, c) => c.copy(applicationId = Some(ApplicationId(x))))
      .optional()
      .text(
        s"Application ID used to interact with the ledger. Defaults to ${Runner.DEFAULT_APPLICATION_ID}"
      )

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
      config: ScriptConfig,
      timeMode: ScriptTimeMode,
  ): ScriptConfig = {
    if (config.timeMode.exists(_ != timeMode)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
      )
    }
    config.copy(timeMode = Some(timeMode))
  }

  private def setMode(
      config: ScriptConfig,
      mode: CliMode,
  ): ScriptConfig = {
    if (config.mode.exists(_ != mode)) {
      throw new IllegalStateException(
        "--script-name and --all are mutually exclusive."
      )
    }
    config.copy(mode = Some(mode))
  }

  def parse(args: Array[String]): Option[ScriptConfig] =
    parser.parse(
      args,
      ScriptConfig(
        darPath = null,
        mode = None,
        ledgerHost = None,
        ledgerPort = None,
        participantConfig = None,
        timeMode = None,
        inputFile = None,
        outputFile = None,
        accessTokenFile = None,
        tlsConfig = TlsConfiguration(false, None, None, None),
        jsonApi = false,
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        applicationId = None,
      ),
    )
}
