// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.io.File
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

import com.daml.caching
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.resources.ResourceOwner
import com.daml.platform.configuration.Readers._
import com.daml.platform.configuration.{IndexConfiguration, MetricsReporter}
import com.daml.ports.Port
import scopt.OptionParser

final case class Config[Extra](
    mode: Mode,
    ledgerId: String,
    archiveFiles: Seq[Path],
    tlsConfig: Option[TlsConfiguration],
    participants: Seq[ParticipantConfig],
    maxInboundMessageSize: Int,
    eventsPageSize: Int,
    stateValueCache: caching.WeightedCache.Configuration,
    lfValueTranslationEventCache: caching.SizedCache.Configuration,
    lfValueTranslationContractCache: caching.SizedCache.Configuration,
    seeding: Seeding,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    extra: Extra,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))
}

object Config {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024
  def createDefault[Extra](extra: Extra): Config[Extra] =
    Config(
      mode = Mode.Run,
      ledgerId = UUID.randomUUID().toString,
      archiveFiles = Vector.empty,
      tlsConfig = None,
      participants = Vector.empty,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      eventsPageSize = IndexConfiguration.DefaultEventsPageSize,
      stateValueCache = caching.WeightedCache.Configuration.none,
      lfValueTranslationEventCache = caching.SizedCache.Configuration.none,
      lfValueTranslationContractCache = caching.SizedCache.Configuration.none,
      seeding = Seeding.Strong,
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      extra = extra,
    )

  def owner[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: Seq[String],
  ): ResourceOwner[Config[Extra]] =
    parse(name, extraOptions, defaultExtra, args)
      .fold[ResourceOwner[Config[Extra]]](ResourceOwner.failed(new ConfigParseException))(
        ResourceOwner.successful)

  def parse[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: Seq[String],
      getEnvVar: String => Option[String] = sys.env.get(_),
  ): Option[Config[Extra]] =
    parser(name, extraOptions, getEnvVar).parse(args, createDefault(defaultExtra)).flatMap {
      case config if config.mode == Mode.Run && config.participants.isEmpty =>
        System.err.println("No --participant provided to run")
        None
      case config =>
        Some(config)
    }

  private def parser[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      getEnvVar: String => Option[String],
  ): OptionParser[Config[Extra]] = {
    val parser: OptionParser[Config[Extra]] = new OptionParser[Config[Extra]](name) {
      head(name)

      opt[Map[String, String]]("participant")
        .unbounded()
        .text("The configuration of a participant. Comma-separated pairs in the form key=value, with mandatory keys: [participant-id, port] and optional keys [address, port-file, server-jdbc-url, max-commands-in-flight, management-service-timeout, run-mode, shard-name]")
        .action((kv, config) => {
          val participantId = ParticipantId.assertFromString(kv("participant-id"))
          val port = Port(kv("port").toInt)
          val address = kv.get("address")
          val portFile = kv.get("port-file").map(new File(_).toPath)
          val runMode: ParticipantRunMode = kv.get("run-mode") match {
            case None => ParticipantRunMode.Combined
            case Some("combined") => ParticipantRunMode.Combined
            case Some("indexer") => ParticipantRunMode.Indexer
            case Some("ledger-api-server") => ParticipantRunMode.LedgerApiServer
            case Some(unknownMode) =>
              throw new RuntimeException(
                s"$unknownMode is not a valid run mode. Valid modes are: combined, indexer, ledger-api-server. Default mode is combined.")
          }
          val jdbcUrlFromEnv =
            kv.get("server-jdbc-url-env").flatMap(getEnvVar(_))
          val jdbcUrl =
            kv.getOrElse(
              "server-jdbc-url",
              jdbcUrlFromEnv.getOrElse(ParticipantConfig.defaultIndexJdbcUrl(participantId)))
          val maxCommandsInFlight = kv.get("max-commands-in-flight").map(_.toInt)
          val managementServiceTimeout = kv
            .get("management-service-timeout")
            .map(Duration.parse)
            .getOrElse(ParticipantConfig.defaultManagementServiceTimeout)
          val shardName = kv.get("shard-name")
          val partConfig = ParticipantConfig(
            runMode,
            participantId,
            shardName,
            address,
            port,
            portFile,
            jdbcUrl,
            allowExistingSchemaForIndex = false,
            maxCommandsInFlight = maxCommandsInFlight,
            managementServiceTimeout = managementServiceTimeout,
          )
          config.copy(participants = config.participants :+ partConfig)
        })

      opt[String]("ledger-id")
        .optional()
        .text("The ID of the ledger. This must be the same each time the ledger is started. Defaults to a random UUID.")
        .action((ledgerId, config) => config.copy(ledgerId = ledgerId))

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
        .text("TLS: The crt file to be used as the trusted root CA.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(trustCertCollectionFile = Some(new File(path)))))
      opt[Boolean]("cert-revocation-checking")
        .optional()
        .text(
          "TLS: enable/disable certificate revocation checks with the OCSP. Disabled by default.")
        .action((checksEnabled, config) =>
          config.withTlsConfig(c => c.copy(enableCertRevocationChecking = checksEnabled)))

      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .text("DAR files to load. Scenarios are ignored. The server starts with an empty ledger by default.")
        .action((file, config) => config.copy(archiveFiles = config.archiveFiles :+ file.toPath))

      opt[Int]("max-inbound-message-size")
        .optional()
        .text(
          s"Max inbound message size in bytes. Defaults to ${Config.DefaultMaxInboundMessageSize}.")
        .action((maxInboundMessageSize, config) =>
          config.copy(maxInboundMessageSize = maxInboundMessageSize))

      opt[Int]("events-page-size")
        .optional()
        .text(
          s"Number of events fetched from the index for every round trip when serving streaming calls. Default is ${IndexConfiguration.DefaultEventsPageSize}.")
        .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize))

      opt[Long]("max-state-value-cache-size")
        .optional()
        .text(
          s"The maximum size of the cache used to deserialize state values, in MB. By default, nothing is cached.")
        .action((maximumStateValueCacheSize, config) =>
          config.copy(stateValueCache =
            config.stateValueCache.copy(maximumWeight = maximumStateValueCacheSize * 1024 * 1024)))

      opt[Long]("max-lf-value-translation-cache-entries")
        .optional()
        .text(
          s"The maximum size of the cache used to deserialize DAML-LF values, in number of allowed entries. By default, nothing is cached.")
        .action((maximumLfValueTranslationCacheEntries, config) =>
          config.copy(
            lfValueTranslationEventCache = config.lfValueTranslationEventCache.copy(
              maximumSize = maximumLfValueTranslationCacheEntries),
            lfValueTranslationContractCache = config.lfValueTranslationContractCache.copy(
              maximumSize = maximumLfValueTranslationCacheEntries),
        ))

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

      cmd("dump-index-metadata")
        .text("Print ledger id, ledger end and integration API version and quit.")
        .children {
          arg[String]("<jdbc-url>...")
            .minOccurs(1)
            .unbounded()
            .text("The JDBC URL to connect to an index database")
            .action((jdbcUrl, config) =>
              config.copy(mode = config.mode match {
                case Mode.Run =>
                  Mode.DumpIndexMetadata(Vector(jdbcUrl))
                case Mode.DumpIndexMetadata(jdbcUrls) =>
                  Mode.DumpIndexMetadata(jdbcUrls :+ jdbcUrl)
              }))
        }

      checkConfig(c => {
        val participantsIdsWithNonUniqueShardNames = c.participants
          .map(pc => pc.participantId -> pc.shardName)
          .groupBy(_._1)
          .map { case (k, v) => (k, v.map(_._2)) }
          .filter { case (_, v) => v.length != v.distinct.length }
          .keys
        if (participantsIdsWithNonUniqueShardNames.nonEmpty)
          failure(
            participantsIdsWithNonUniqueShardNames.mkString(
              "The following participant IDs are duplicate, but the individual shards don't have unique names: ",
              ",",
              ". Use the optional 'shard-name' key when specifying horizontally scaled participants."
            ))
        else
          success
      })

      help("help").text(s"$name as a service.")
    }
    extraOptions(parser)
    parser
  }

}
