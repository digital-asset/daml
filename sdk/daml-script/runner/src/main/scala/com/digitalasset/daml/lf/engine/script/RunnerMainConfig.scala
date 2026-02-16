// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import java.nio.file.{Path, Paths}
import java.io.File

import com.digitalasset.daml.lf.data.Ref
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
    // While we do have a default user id, we
    // want to differentiate between not specifying the user id
    // and specifying the default for better error messages.
    userId: Option[Option[Ref.UserId]],
    uploadDar: Boolean,
    resultMode: RunnerMainConfig.ResultMode,
)

object RunnerMainConfig {
  val DefaultTimeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  // We default to MAXINT as we rely on the ledger to manage the message size
  val DefaultMaxInboundMessageSize: Int = Int.MaxValue

  sealed trait RunMode
  object RunMode {
    final case class RunSingle(
        scriptId: String,
        inputFile: Option[File],
        outputFile: Option[File],
    ) extends RunMode
    final case class RunExcluding(
        scriptIds: List[String]
    ) extends RunMode
    final case class RunIncluding(
        scriptIds: List[String]
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

  sealed trait ResultMode
  object ResultMode {
    final case object Text extends ResultMode
    final case class Json(path: File) extends ResultMode
  }
}

private[script] case class RunnerMainConfigIntermediate(
    darPath: File,
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
    // While we do have a default user id, we
    // want to differentiate between not specifying the user id
    // and specifying the default for better error messages.
    userId: Option[Option[Ref.UserId]],
    uploadDar: Boolean,
    resultMode: RunnerMainConfig.ResultMode,
    runAll: Boolean,
    includeScriptNames: List[String],
    excludeScriptNames: List[String],
) {

  def getRunMode: Either[String, RunnerMainConfig.RunMode] =
    (
      runAll,
      includeScriptNames,
      excludeScriptNames,
      inputFile.isDefined || outputFile.isDefined,
    ) match {
      case (true, List(), excludes, false) =>
        Right(RunnerMainConfig.RunMode.RunExcluding(excludes))
      case (true, _, _, false) =>
        Left("--all and --script-name flags are incompatible")
      case (true, _, _, true) =>
        Left("--all and --input-file/--output-file are incompatible")
      case (false, includes, excludes, useIO) =>
        val scriptNames = includes diff excludes
        scriptNames match {
          case List() if includes.isEmpty => Left("--all or --script-name must be specified.")
          case List() =>
            Left("--script-name and --skip-script-name flags fully cancel out to no tests")
          case List(singleTest) =>
            Right(RunnerMainConfig.RunMode.RunSingle(singleTest, inputFile, outputFile))
          case _ if useIO =>
            Left("Cannot use --input-file/--output-file when more than one test is specified")
          case _ => Right(RunnerMainConfig.RunMode.RunIncluding(scriptNames))
        }
    }

  def validateUploadDar(participantMode: ParticipantMode): Either[String, Unit] =
    Either.cond(
      !uploadDar || participantMode != ParticipantMode.IdeLedgerParticipant(),
      (),
      "Cannot upload dar to IDELedger.",
    )

  def toRunnerMainConfig: Either[String, RunnerMainConfig] =
    for {
      runMode <- getRunMode
      participantMode = this.getLedgerMode
      resolvedTimeMode = timeMode.getOrElse(RunnerMainConfig.DefaultTimeMode)
      _ <- validateUploadDar(participantMode)
      config = RunnerMainConfig(
        darPath = darPath,
        runMode = runMode,
        participantMode = participantMode,
        timeMode = resolvedTimeMode,
        accessTokenFile = accessTokenFile,
        tlsConfig = tlsConfig,
        maxInboundMessageSize = maxInboundMessageSize,
        userId = userId,
        uploadDar = uploadDar,
        resultMode = resultMode,
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
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private val parser = new scopt.OptionParser[RunnerMainConfigIntermediate]("script-runner") {
    head("script-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the script")

    opt[String]("script-name")
      .optional()
      .unbounded()
      .action((t, c) => c.copy(includeScriptNames = c.includeScriptNames :+ t))
      .text("Identifier of the script that should be run in the format Module.Name:Entity.Name")

    opt[String]("skip-script-name")
      .optional()
      .unbounded()
      .action((t, c) => c.copy(excludeScriptNames = c.excludeScriptNames :+ t))
      .text("Identifier of the script that should be skipped when using --all")

    opt[Unit]("all")
      .optional()
      .action((_, c) => c.copy(runAll = true))
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
        "EXPERIMENTAL: Admin port. Used only for vetting and unvetting dars."
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

    opt[String]("user-id")
      .action((x, c) =>
        c.copy(userId = Some(Some(x).filter(_.nonEmpty).map(Ref.UserId.assertFromString)))
      )
      .optional()
      .text(
        s"User ID used to interact with the ledger. Defaults to ${Runner.DEFAULT_USER_ID}"
      )

    opt[Boolean]("upload-dar")
      .action((x, c) => c.copy(uploadDar = x))
      .optional()
      .text(
        s"Upload the dar before running. Only available over GRPC. Defaults to false"
      )

    opt[File]("json-test-summary")
      .action((path, c) => c.copy(resultMode = RunnerMainConfig.ResultMode.Json(path)))
      .text(
        s"Put test summary into a file in json format. Only works when running multiple cases."
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

  private[script] def parse(args: Array[String]): Option[RunnerMainConfigIntermediate] =
    parser.parse(
      args,
      RunnerMainConfigIntermediate(
        darPath = null,
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
        userId = None,
        uploadDar = false,
        resultMode = RunnerMainConfig.ResultMode.Text,
        runAll = false,
        includeScriptNames = List(),
        excludeScriptNames = List(),
      ),
    )
}
