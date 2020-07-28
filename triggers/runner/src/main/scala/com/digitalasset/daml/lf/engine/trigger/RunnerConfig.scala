// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Duration
import scala.util.Try

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.services.time.TimeProviderType

case class RunnerConfig(
    darPath: Path,
    // If true, we will only list the triggers in the DAR and exit.
    listTriggers: Boolean,
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerParty: String,
    maxInboundMessageSize: Int,
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeProviderType: Option[TimeProviderType],
    commandTtl: Duration,
    accessTokenFile: Option[Path],
    tlsConfig: Option[TlsConfiguration],
)

object RunnerConfig {
  val DefaultMaxInboundMessageSize: Int = 4194304
  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val readable = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (readable) Right(()) else Left(message)
  }

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
      .action((t, c) => c.copy(ledgerParty = t))
      .text("Ledger party")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}")

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
      .text("File from which the access token will be read, required to interact with an authenticated ledger")

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key.")
      .validate(path => validatePath(path, "The file specified via --pem does not exist"))
      .action((path, arguments) =>
        arguments.copy(tlsConfig = arguments.tlsConfig.fold(
          Some(TlsConfiguration(true, None, Some(new File(path)), None)))(c =>
          Some(c.copy(keyFile = Some(new File(path)))))))

    opt[String]("crt")
      .optional()
      .text("TLS: The crt file to be used as the cert chain. Required for client authentication.")
      .validate(path => validatePath(path, "The file specified via --crt does not exist"))
      .action((path, arguments) =>
        arguments.copy(tlsConfig = arguments.tlsConfig.fold(
          Some(TlsConfiguration(true, None, Some(new File(path)), None)))(c =>
          Some(c.copy(keyFile = Some(new File(path)))))))

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the the trusted root CA.")
      .validate(path => validatePath(path, "The file specified via --cacrt does not exist"))
      .action((path, arguments) =>
        arguments.copy(tlsConfig = arguments.tlsConfig.fold(
          Some(TlsConfiguration(true, None, None, Some(new File(path)))))(c =>
          Some(c.copy(trustCertCollectionFile = Some(new File(path)))))))

    opt[Unit]("tls")
      .optional()
      .text("TLS: Enable tls. This is redundant if --pem, --crt or --cacrt are set")
      .action((path, arguments) =>
        arguments.copy(tlsConfig =
          arguments.tlsConfig.fold(Some(TlsConfiguration(true, None, None, None)))(Some(_))))

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
    })
  }

  private def setTimeProviderType(
      config: RunnerConfig,
      timeProviderType: TimeProviderType,
  ): RunnerConfig = {
    if (config.timeProviderType.exists(_ != timeProviderType)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous.")
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
        ledgerParty = null,
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        timeProviderType = None,
        commandTtl = Duration.ofSeconds(30L),
        accessTokenFile = None,
        tlsConfig = None,
      )
    )
}
