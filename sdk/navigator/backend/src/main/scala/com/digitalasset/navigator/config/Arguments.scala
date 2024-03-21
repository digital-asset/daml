// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.config

import java.io.File
import java.nio.file.{Path, Paths}

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.navigator.time.TimeProviderType
import scopt.{OptionDef, OptionParser}

import scala.util.Try

sealed abstract class Command
case object ShowUsage extends Command
case object RunServer extends Command
case object CreateConfig extends Command
case object DumpGraphQLSchema extends Command

case class Arguments(
    port: Int = 4000,
    assets: Option[String] = None,
    command: Command = ShowUsage,
    time: TimeProviderType = TimeProviderType.Auto,
    participantHost: String = "localhost",
    participantPort: Int = 6865,
    tlsConfig: Option[TlsConfiguration] = None,
    accessTokenFile: Option[Path] = None,
    configFile: Option[Path] = None,
    startWebServer: Boolean = false,
    useDatabase: Boolean = false,
    ledgerInboundMessageSizeMax: Int = 50 * 1024 * 1024, // 50 MiB
    ignoreProjectParties: Boolean = false,
    enableUserManagement: Boolean = true,
)

trait ArgumentsHelper { self: OptionParser[Arguments] =>
  def hostname: OptionDef[String, Arguments] =
    arg[String]("<host>")
      .text(
        s"hostname or IP address of the Ledger API server (default ${Arguments.default.participantHost})"
      )
      .optional()
      .action((ip, arguments) =>
        arguments.copy(
          participantHost = ip
        )
      )

  def port: OptionDef[Int, Arguments] =
    arg[Int]("<port>")
      .text(s"port number of the Ledger API server (default ${Arguments.default.participantPort})")
      .optional()
      .action((port, arguments) =>
        arguments.copy(
          participantPort = port
        )
      )
}

object Arguments {

  val default = Arguments()

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val readable = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (readable) Right(()) else Left(message)
  }

  private val crtConfig = (path: String, arguments: Arguments) =>
    arguments.copy(
      tlsConfig =
        arguments.tlsConfig.fold(Some(TlsConfiguration(true, Some(new File(path)), None, None)))(
          c => Some(c.copy(keyCertChainFile = Some(new File(path))))
        )
    )

  private def argumentParser(defaultConfigFile: Path) =
    new OptionParser[Arguments]("navigator") with ArgumentsHelper {
      help("help").abbr("h").text("prints this usage text")

      opt[Int]("port")
        .text(s"port number on which the server should listen (default ${Arguments.default.port})")
        .action((port, arguments) => arguments.copy(port = port))

      opt[String]("assets")
        .text("folder where frontend assets are available")
        .action((assets, arguments) => arguments.copy(assets = Some(assets)))

      opt[String]("access-token-file")
        .text(
          s"provide the path from which the access token will be read, required to interact with an authenticated ledger, no default"
        )
        .action((path, arguments) => arguments.copy(accessTokenFile = Some(Paths.get(path))))
        .optional()

      opt[String]('c', "config-file")
        .text(s"set the configuration file default: ${defaultConfigFile}")
        .action((path, arguments) => arguments.copy(configFile = Some(Paths.get(path))))

      opt[TimeProviderType]('t', "time")
        .text(s"Time provider. Valid values are: ${TimeProviderType.acceptedValues
            .mkString(", ")}. Default: ${Arguments.default.time.name}")
        .action((t, arguments) => arguments.copy(time = t))

      // TODO: the 4 following TLS options can be defined by TlsConfigurationCli instead

      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .validate(path => validatePath(path, "The file specified via --pem does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig =
            arguments.tlsConfig.fold(
              Some(TlsConfiguration(true, None, Some(new File(path)), None))
            )(c => Some(c.copy(keyFile = Some(new File(path)))))
          )
        )

      opt[String]("crt")
        .optional()
        .text("TLS: The crt file to be used as the cert chain. Required for client authentication.")
        .validate(path => validatePath(path, "The file specified via --crt does not exist"))
        .action(crtConfig)

      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the trusted root CA.")
        .validate(path => validatePath(path, "The file specified via --cacrt does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig =
            arguments.tlsConfig.fold(
              Some(TlsConfiguration(true, None, None, Some(new File(path))))
            )(c => Some(c.copy(trustCertCollectionFile = Some(new File(path)))))
          )
        )

      opt[Unit]("tls")
        .optional()
        .text("TLS: Enable tls. This is redundant if --pem, --crt or --cacrt are set")
        .action((_, arguments) =>
          arguments.copy(tlsConfig =
            arguments.tlsConfig.fold(Some(TlsConfiguration(true, None, None, None)))(Some(_))
          )
        )

      opt[Unit]("database")
        .hidden()
        .text("EXPERIMENTAL: use an SQLite data store")
        .action((_, arguments) =>
          arguments.copy(
            useDatabase = true
          )
        )

      opt[Int]("ledger-api-inbound-message-size-max")
        .text(
          s"Maximum message size in bytes from the ledger API. Default is ${Arguments.default.ledgerInboundMessageSizeMax}."
        )
        .valueName("<bytes>")
        .validate(x => Either.cond(x > 0, (), "Buffer size must be positive"))
        .action((ledgerInboundMessageSizeMax, arguments) => {
          arguments.copy(
            ledgerInboundMessageSizeMax = ledgerInboundMessageSizeMax
          )
        })

      opt[Unit]("ignore-project-parties")
        .hidden()
        .optional()
        .text(
          "Ignore the parties specified in the project configuration file and query the ledger for parties instead."
        )
        .action((_, arguments) => arguments.copy(ignoreProjectParties = true))

      opt[Boolean]("feature-user-management")
        .optional()
        .text(
          "By default, the login screen is now populated by quering the user mgmt service. Disable to query party mgmt instead (the pre-2.0 default)."
        )
        .action((enabled, arguments) => arguments.copy(enableUserManagement = enabled))

      cmd("server")
        .text("serve data from platform")
        .action((_, arguments) =>
          arguments.copy(
            command = RunServer,
            startWebServer = true,
          )
        )
        .children(hostname, port)

      cmd("dump-graphql-schema")
        .text("Early Access (Labs). Dumps the full GraphQL schema to stdout")
        .action((_, arguments) => arguments.copy(command = DumpGraphQLSchema))

      cmd("create-config")
        .text("Creates a template configuration file")
        .action((_, arguments) => arguments.copy(command = CreateConfig))
    }

  def parse(args: Array[String], defaultConfigFile: Path): Option[Arguments] =
    this.argumentParser(defaultConfigFile).parse(args, Arguments.default)

  def showUsage(defaultConfigFile: Path): Unit = {
    val parser = this.argumentParser(defaultConfigFile)
    parser.displayToOut(parser.usage)
  }
}
