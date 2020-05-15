// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.{Path, Paths}
import java.io.File
import java.time.Duration
import scala.util.Try

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.services.time.TimeProviderType

case class RunnerConfig(
    darPath: File,
    scriptIdentifier: String,
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeProviderType: Option[TimeProviderType],
    commandTtl: Duration,
    inputFile: Option[File],
    outputFile: Option[File],
    accessTokenFile: Option[Path],
    tlsConfig: Option[TlsConfiguration],
    jsonApi: Boolean,
    maxInboundMessageSize: Int,
)

object RunnerConfig {

  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  val DefaultMaxInboundMessageSize: Int = 4194304

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val readable = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (readable) Right(()) else Left(message)
  }

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

    opt[Unit]("json-api")
      .action { (t, c) =>
        c.copy(jsonApi = true)
      }
      .text("Run DAML Script via the HTTP JSON API instead of via gRPC (experimental).")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to $DefaultMaxInboundMessageSize")

    help("help").text("Print this usage text")

    checkConfig(c => {
      if (c.ledgerHost.isDefined != c.ledgerPort.isDefined) {
        failure("Must specify both --ledger-host and --ledger-port")
      } else if (c.ledgerHost.isDefined && c.participantConfig.isDefined) {
        failure("Cannot specify both --ledger-host and --participant-config")
      } else if (c.ledgerHost.isEmpty && c.participantConfig.isEmpty) {
        failure("Must specify either --ledger-host or --participant-config")
      } else if (c.jsonApi && c.accessTokenFile.isEmpty) {
        failure("The json-api requires an access token")
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
        scriptIdentifier = null,
        ledgerHost = None,
        ledgerPort = None,
        participantConfig = None,
        timeProviderType = None,
        commandTtl = Duration.ofSeconds(30L),
        inputFile = None,
        outputFile = None,
        accessTokenFile = None,
        tlsConfig = None,
        jsonApi = false,
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
      )
    )
}
