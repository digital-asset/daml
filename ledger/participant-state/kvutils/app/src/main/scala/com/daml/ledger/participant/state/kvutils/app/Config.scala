// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.io.File
import java.nio.file.Path
import java.time.Duration

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.configuration.MetricsReporter
import com.digitalasset.platform.configuration.Readers._
import com.digitalasset.ports.Port
import com.digitalasset.resources.ProgramResource.SuppressedStartupException
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

final case class Config[Extra](
    ledgerId: Option[String],
    archiveFiles: Seq[Path],
    tlsConfig: Option[TlsConfiguration],
    participants: Seq[ParticipantConfig],
    seeding: Seeding,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    eventsPageSize: Int,
    extra: Extra,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))
}

case class ParticipantConfig(
    participantId: ParticipantId,
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    serverJdbcUrl: String,
    allowExistingSchemaForIndex: Boolean,
)

object ParticipantConfig {
  def defaultIndexJdbcUrl(participantId: ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"
}

object Config {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  val DefaultEventsPageSize = 1000

  def default[Extra](extra: Extra): Config[Extra] =
    Config(
      ledgerId = None,
      archiveFiles = Vector.empty,
      tlsConfig = None,
      participants = Vector.empty,
      seeding = Seeding.Strong,
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      eventsPageSize = DefaultEventsPageSize,
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
    val parser: OptionParser[Config[Extra]] = new OptionParser[Config[Extra]](name) {
      head(name)

      opt[Map[String, String]]("participant")
        .minOccurs(1)
        .unbounded()
        .text("The configuration of a participant. Comma-separated key-value pairs, with mandatory keys: [participant-id, port] and optional keys [address, port-file, server-jdbc-url]")
        .action((kv, config) => {
          val participantId = ParticipantId.assertFromString(kv("participant-id"))
          val port = Port(kv("port").toInt)
          val address = kv.get("address")
          val portFile = kv.get("port-file").map(new File(_).toPath)
          val jdbcUrl =
            kv.getOrElse("server-jdbc-url", ParticipantConfig.defaultIndexJdbcUrl(participantId))
          val partConfig = ParticipantConfig(
            participantId,
            address,
            port,
            portFile,
            jdbcUrl,
            allowExistingSchemaForIndex = false)
          config.copy(participants = config.participants :+ partConfig)
        })
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

      opt[Int]("events-page-size")
        .optional()
        .text(
          s"Number of events fetched from the index for every round trip when serving streaming calls. Default is ${Config.DefaultEventsPageSize}.")
        .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize))

      private val seedingMap =
        Map[String, Seeding]("testing-weak" -> Seeding.Weak, "strong" -> Seeding.Strong)

      opt[String]("contract-id-seeding")
        .optional()
        .text(s"""Set the seeding of contract ids. Possible values are ${seedingMap.keys.mkString(
          ",")}. Default is "strong".""")
        .validate(
          v =>
            Either.cond(
              seedingMap.contains(v.toLowerCase),
              (),
              s"seeding must be ${seedingMap.keys.mkString(",")}"))
        .action((text, config) => config.copy(seeding = seedingMap(text)))
        .hidden()

      opt[MetricsReporter]("metrics-reporter")
        .optional()
        .action((reporter, config) => config.copy(metricsReporter = Some(reporter)))
        .hidden()

      opt[Duration]("metrics-reporting-interval")
        .optional()
        .action((interval, config) => config.copy(metricsReportingInterval = interval))
        .hidden()

      help("help").text(s"$name as a service.")
    }
    extraOptions(parser)
    parser
  }

  class ConfigParseException extends RuntimeException with SuppressedStartupException
}
