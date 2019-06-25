// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.config

import java.io.File
import java.nio.file.{Path, Paths}

import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.navigator.time.TimeProviderType
import scopt.{OptionDef, OptionParser}

import scala.util.Try

sealed abstract class Command
final case object ShowUsage extends Command
final case object RunServer extends Command
final case object CreateConfig extends Command
final case object DumpGraphQLSchema extends Command

case class Arguments(
    port: Int = 4000,
    assets: Option[String] = None,
    command: Command = ShowUsage,
    time: TimeProviderType = TimeProviderType.Auto,
    platformIp: String = "localhost",
    platformPort: Int = 6865,
    tlsConfig: Option[TlsConfiguration] = None,
    requirePassword: Boolean = false,
    configFile: Option[Path] = None,
    startConsole: Boolean = false,
    startWebServer: Boolean = false,
    useDatabase: Boolean = false,
    ledgerInboundMessageSizeMax: Int = 50 * 1024 * 1024 // 50 MiB
)

trait ArgumentsHelper { self: OptionParser[Arguments] =>
  def hostname: OptionDef[String, Arguments] =
    arg[String]("<host>")
      .text("hostname or IP address of the platform server (default localhost)")
      .optional()
      .action(
        (ip, arguments) =>
          arguments.copy(
            platformIp = ip
        ))

  def port: OptionDef[Int, Arguments] =
    arg[Int]("<port>")
      .text("port number of the platform server (default 6865)")
      .optional()
      .action(
        (port, arguments) =>
          arguments.copy(
            platformPort = port
        ))
}

object Arguments {

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val readable = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (readable) Right(()) else Left(message)
  }

  private val crtConfig = (path: String, arguments: Arguments) =>
    arguments.copy(
      tlsConfig = arguments.tlsConfig.fold(
        Some(TlsConfiguration(true, Some(new File(path)), None, None)))(c =>
        Some(c.copy(keyCertChainFile = Some(new File(path))))))

  private def argumentParser(defaultConfigFile: Path) =
    new OptionParser[Arguments]("navigator") with ArgumentsHelper {
      help("help").abbr("h").text("prints this usage text")

      opt[Int]("port")
        .text("port number on which the server should listen (default 4000)")
        .action((port, arguments) => arguments.copy(port = port))

      opt[String]("assets")
        .text("folder where frontend assets are available")
        .action((assets, arguments) => arguments.copy(assets = Some(assets)))

      opt[Unit]('w', "require-password")
        .text("if true then the password is required to login")
        .action((_, arguments) => arguments.copy(requirePassword = true))

      opt[String]('c', "config-file")
        .text(s"set the configuration file default: ${defaultConfigFile}")
        .action((path, arguments) => arguments.copy(configFile = Some(Paths.get(path))))

      opt[TimeProviderType]('t', "time")
        .text(
          s"Time provider. Valid values are: ${TimeProviderType.acceptedValues.mkString(", ")}. Default: static")
        .action((t, arguments) => arguments.copy(time = t))

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
        .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
        .validate(path => validatePath(path, "The file specified via --crt does not exist"))
        .action(crtConfig)

      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the the trusted root CA.")
        .validate(path => validatePath(path, "The file specified via --cacrt does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig = arguments.tlsConfig.fold(
            Some(TlsConfiguration(true, None, None, Some(new File(path)))))(c =>
            Some(c.copy(trustCertCollectionFile = Some(new File(path)))))))

      opt[Unit]("database")
        .hidden()
        .text("EXPERIMENTAL: use an SQLite data store")
        .action(
          (_, arguments) =>
            arguments.copy(
              useDatabase = true
          ))

      opt[Int]("ledger-api-inbound-message-size-max")
        .hidden()
        .text("Maximum message size from the ledger API. Default is 52428800 (50MiB).")
        .valueName("<bytes>")
        .validate(x => Either.cond(x > 0, (), "Buffer size must be positive"))
        .action((x, arguments) =>
          arguments.copy(
            ledgerInboundMessageSizeMax = x
        ))

      cmd("server")
        .text("serve data from platform")
        .action(
          (_, arguments) =>
            arguments.copy(
              command = RunServer,
              startWebServer = true
          ))
        .children(hostname, port)

      cmd("console")
        .text("start the console")
        .action(
          (_, arguments) =>
            arguments.copy(
              command = RunServer,
              useDatabase = true,
              startConsole = true
          ))
        .children(hostname, port)

      cmd("dump-graphql-schema")
        .text("Dumps the full GraphQL schema to stdout")
        .action((_, arguments) => arguments.copy(command = DumpGraphQLSchema))

      cmd("create-config")
        .text("Creates a template configuration file")
        .action((_, arguments) => arguments.copy(command = CreateConfig))
    }

  def parse(args: Array[String], defaultConfigFile: Path): Option[Arguments] =
    this.argumentParser(defaultConfigFile).parse(args, Arguments())

  def showUsage(defaultConfigFile: Path): Unit =
    this.argumentParser(defaultConfigFile).showUsage()
}
