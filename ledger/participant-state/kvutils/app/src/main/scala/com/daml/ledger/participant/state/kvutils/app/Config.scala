// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.io.File
import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.resources.ProgramResource.SuppressedStartupException
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

case class Config[Extra](
    participantId: ParticipantId,
    address: Option[String],
    port: Int,
    portFile: Option[Path],
    serverJdbcUrl: String,
    ledgerId: Option[String],
    archiveFiles: Seq[Path],
    allowExistingSchemaForIndex: Boolean,
    tlsConfig: Option[TlsConfiguration],
    extra: Extra,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))
}

object Config {
  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  def default[Extra](extra: Extra): Config[Extra] =
    Config(
      participantId = ParticipantId.assertFromString("example"),
      address = None,
      port = 6865,
      portFile = None,
      serverJdbcUrl = "jdbc:h2:mem:server;db_close_delay=-1;db_close_on_exit=false",
      ledgerId = None,
      archiveFiles = Vector.empty,
      allowExistingSchemaForIndex = false,
      tlsConfig = None,
      extra = extra,
    )

  def owner[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: Seq[String],
  ): ResourceOwner[Config[Extra]] =
    parse(name, extraOptions, defaultExtra, args)
      .fold[ResourceOwner[Config[Extra]]](ResourceOwner.failed(new Config.ConfigParseException))(
        ResourceOwner.successful)

  def parse[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: Seq[String],
  ): Option[Config[Extra]] =
    parser(name, extraOptions).parse(args, default(defaultExtra))

  private def parser[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
  ): OptionParser[Config[Extra]] = {
    val parser = new OptionParser[Config[Extra]](name) {
      head(name)

      opt[String](name = "participant-id")
        .optional()
        .text("The participant ID given to all components of the ledger API server.")
        .action((participantId, config) =>
          config.copy(participantId = ParticipantId.assertFromString(participantId)))

      opt[String]("address")
        .optional()
        .text("The address on which to run the Ledger API Server.")
        .action((address, config) => config.copy(address = Some(address)))

      opt[Int]("port")
        .optional()
        .text("The port on which to run the Ledger API Server.")
        .action((port, config) => config.copy(port = port))

      opt[File]("port-file")
        .optional()
        .hidden()
        .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
        .action((file, config) => config.copy(portFile = Some(file.toPath)))

      opt[String]("server-jdbc-url")
        .text(
          "The JDBC URL to the database used for the Ledger API Server and the Indexer Server. Defaults to an in-memory H2 database.")
        .action((serverJdbcUrl, config) => config.copy(serverJdbcUrl = serverJdbcUrl))

      opt[String]("ledger-id")
        .text(
          "The ID of the ledger. This must be the same each time the ledger is started. Defaults to a random UUID.")
        .action((ledgerId, config) => config.copy(ledgerId = Some(ledgerId)))

      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .action((path, config) => config.withTlsConfig(c => c.copy(keyFile = Some(new File(path)))))
      opt[String]("crt")
        .optional()
        .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(keyCertChainFile = Some(new File(path)))))
      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the the trusted root CA.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(trustCertCollectionFile = Some(new File(path)))))

      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .text("DAR files to load. Scenarios are ignored. The server starts with an empty ledger by default.")
        .action((file, config) => config.copy(archiveFiles = config.archiveFiles :+ file.toPath))

      help("help").text(s"$name as a service.")
    }
    extraOptions(parser)
    parser
  }

  class ConfigParseException extends RuntimeException with SuppressedStartupException
}
