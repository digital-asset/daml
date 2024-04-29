// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.{Path, Paths}
import java.io.File

import com.daml.lf.data.Ref
import com.daml.tls.{TlsConfiguration, TlsConfigurationCli}

case class RunnerMainConfig(
    darPath: File,
    runMode: RunnerMainConfig.RunMode,
    participantMode: ParticipantMode,
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeMode: ScriptTimeMode,
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    maxInboundMessageSize: Int,
    // While we do have a default application id, we
    // want to differentiate between not specifying the application id
    // and specifying the default for better error messages.
    applicationId: Option[Option[Ref.ApplicationId]],
    uploadDar: Boolean,
    enableContractUpgrading: Boolean,
)

object RunnerMainConfig {
  val DefaultTimeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  val DefaultMaxInboundMessageSize: Int = 4194304

  sealed trait RunMode
  object RunMode {
    final case object RunAll extends RunMode
    final case class RunOne(
        scriptId: String,
        inputFile: Option[File],
        outputFile: Option[File],
    ) extends RunMode
  }

  def parse(args: Array[String]): Option[RunnerMainConfig] =
    for {
      intermediate <- RunnerMainConfigIntermediate.parse(args)
      config <-
        intermediate.toRunnerMainConfig.fold(
          err => {
            System.err.println(err)
            None
          },
          Some(_),
        )
    } yield config
}

private[script] case class RunnerMainConfigIntermediate(
    darPath: File,
    mode: Option[RunnerMainConfigIntermediate.CliMode],
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    adminPort: Option[Int],
    participantConfig: Option[File],
    isIdeLedger: Boolean,
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeMode: Option[ScriptTimeMode],
    inputFile: Option[File],
    outputFile: Option[File],
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    maxInboundMessageSize: Int,
    // While we do have a default application id, we
    // want to differentiate between not specifying the application id
    // and specifying the default for better error messages.
    applicationId: Option[Option[Ref.ApplicationId]],
    // Legacy behaviour is to upload the dar when using --all and over grpc. None represents that behaviour
    // We will drop this for daml3, such that we default to not uploading.
    uploadDar: Option[Boolean],
    enableContractUpgrading: Boolean,
) {
  import RunnerMainConfigIntermediate._

  def getRunMode(cliMode: CliMode): Either[String, RunnerMainConfig.RunMode] =
    cliMode match {
      case CliMode.RunOne(id) =>
        Right(RunnerMainConfig.RunMode.RunOne(id, inputFile, outputFile))
      case CliMode.RunAll =>
        def incompatible(name: String, isValid: Boolean): Either[String, Unit] =
          Either.cond(isValid, (), s"--${name} is incompatible with --all")
        for {
          _ <- incompatible("input-file", inputFile.isEmpty)
          _ <- incompatible("output-file", outputFile.isEmpty)
        } yield RunnerMainConfig.RunMode.RunAll
    }

  def resolveUploadDar(
      participantMode: ParticipantMode,
      cliMode: CliMode,
  ): Either[String, Boolean] =
    (participantMode, uploadDar) match {
      case (ParticipantMode.IdeLedgerParticipant(), Some(true)) =>
        Left("Cannot upload dar to IDELedger.")
      case (ParticipantMode.IdeLedgerParticipant(), _) =>
        // We don't need to upload the dar when using the IDE ledger
        Right(false)
      case (_, Some(v)) => Right(v)
      case (_, None) =>
        Right(cliMode match {
          case CliMode.RunOne(_) => false
          case CliMode.RunAll => {
            println(
              """WARNING: Implicitly using the legacy behaviour of uploading the DAR when using --all over GRPC.
              |This behaviour will be removed for daml3. Please use the explicit `--upload-dar yes` option.
            """.stripMargin
            )
            true
          }
        })
    }

  def toRunnerMainConfig: Either[String, RunnerMainConfig] =
    for {
      cliMode <- mode.toRight("Either --script-name or --all must be specified")
      participantMode = this.getLedgerMode
      resolvedTimeMode = timeMode.getOrElse(RunnerMainConfig.DefaultTimeMode)
      runMode <- getRunMode(cliMode)
      resolvedUploadDar <- resolveUploadDar(participantMode, cliMode)
      config = RunnerMainConfig(
        darPath = darPath,
        runMode = runMode,
        participantMode = participantMode,
        timeMode = resolvedTimeMode,
        accessTokenFile = accessTokenFile,
        tlsConfig = tlsConfig,
        maxInboundMessageSize = maxInboundMessageSize,
        applicationId = applicationId,
        uploadDar = resolvedUploadDar,
        enableContractUpgrading = enableContractUpgrading,
      )
    } yield config

  private def getLedgerMode: ParticipantMode =
    (ledgerHost, ledgerPort, adminPort, participantConfig, isIdeLedger) match {
      case (Some(host), Some(port), oAdminPort, None, false) =>
        ParticipantMode.RemoteParticipantHost(host, port, oAdminPort)
      case (None, None, None, Some(participantConfig), false) =>
        ParticipantMode.RemoteParticipantConfig(participantConfig)
      case (None, None, None, None, true) => ParticipantMode.IdeLedgerParticipant()
      case _ => throw new IllegalStateException("Unsupported combination of ledger modes")
    }
}

private[script] object RunnerMainConfigIntermediate {
  sealed abstract class CliMode extends Product with Serializable
  object CliMode {
    // Run a single script in the DAR
    final case class RunOne(scriptId: String) extends CliMode
    // Run all scripts in the DAR
    final case object RunAll extends CliMode
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private val parser = new scopt.OptionParser[RunnerMainConfigIntermediate]("script-runner") {
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
        "Ledger hostname."
      )

    opt[Int]("ledger-port")
      .optional()
      .action((t, c) => c.copy(ledgerPort = Some(t)))
      .text(
        "Ledger port."
      )

    opt[Int]("admin-port")
      .hidden()
      .optional()
      .action((t, c) => c.copy(adminPort = Some(t)))
      .text(
        "EXPERIMENTAL: Admin port. Used only for vetting and unvetting dars in daml3-script."
      )

    opt[File]("participant-config")
      .optional()
      .action((t, c) => c.copy(participantConfig = Some(t)))
      .text("File containing the participant configuration in JSON format")

    opt[Unit]("ide-ledger")
      .action((_, c) => c.copy(isIdeLedger = true))
      .text("Runs the script(s) in a simulated ledger.")

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

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${RunnerMainConfig.DefaultMaxInboundMessageSize}"
      )

    opt[String]("application-id")
      .action((x, c) =>
        c.copy(applicationId =
          Some(Some(x).filter(_.nonEmpty).map(Ref.ApplicationId.assertFromString))
        )
      )
      .optional()
      .text(
        s"Application ID used to interact with the ledger. Defaults to ${Runner.DEFAULT_APPLICATION_ID}"
      )

    opt[Boolean]("upload-dar")
      .action((x, c) => c.copy(uploadDar = Some(x)))
      .optional()
      .text(
        s"Uploads the dar before running. Only available over GRPC. Default behaviour is to upload with --all, not with --script-name."
      )

    help("help").text("Print this usage text")

    checkConfig(c => {
      if (c.ledgerHost.isDefined != c.ledgerPort.isDefined) {
        failure("Must specify both --ledger-host and --ledger-port")
      } else if (
        List(c.ledgerHost.isDefined, c.participantConfig.isDefined, c.isIdeLedger)
          .count(identity) != 1
      ) {
        failure(
          "Must specify one and only one of --ledger-host, --participant-config, --ide-ledger"
        )
      } else {
        success
      }
    })

  }

  private def setTimeMode(
      config: RunnerMainConfigIntermediate,
      timeMode: ScriptTimeMode,
  ): RunnerMainConfigIntermediate = {
    if (config.timeMode.exists(_ != timeMode)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
      )
    }
    config.copy(timeMode = Some(timeMode))
  }

  private def setMode(
      config: RunnerMainConfigIntermediate,
      mode: CliMode,
  ): RunnerMainConfigIntermediate = {
    if (config.mode.exists(_ != mode)) {
      throw new IllegalStateException(
        "--script-name and --all are mutually exclusive."
      )
    }
    config.copy(mode = Some(mode))
  }

  private[script] def parse(args: Array[String]): Option[RunnerMainConfigIntermediate] =
    parser.parse(
      args,
      RunnerMainConfigIntermediate(
        darPath = null,
        mode = None,
        ledgerHost = None,
        ledgerPort = None,
        adminPort = None,
        participantConfig = None,
        isIdeLedger = false,
        timeMode = None,
        inputFile = None,
        outputFile = None,
        accessTokenFile = None,
        tlsConfig = TlsConfiguration(false, None, None, None),
        maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
        applicationId = None,
        uploadDar = None,
        enableContractUpgrading = false,
      ),
    )
}
