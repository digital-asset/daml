// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.io.File
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import com.daml.caching
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.kvutils.app.Config.EngineMode
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.resources.ResourceOwner
import com.daml.platform.configuration.Readers._
import com.daml.platform.configuration.{CommandConfiguration, IndexConfiguration, MetricsReporter}
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import scopt.OptionParser

import scala.concurrent.duration.FiniteDuration

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
    trackerRetentionPeriod: FiniteDuration,
    engineMode: EngineMode,
    enableAppendOnlySchema: Boolean, // TODO append-only: remove after removing support for the current (mutating) schema
    enableMutableContractStateCache: Boolean,
    extra: Extra,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))
}

object Config {
  val DefaultPort: Port = Port(6865)
  val DefaultTrackerRetentionPeriod: FiniteDuration =
    CommandConfiguration.DefaultTrackerRetentionPeriod

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
      trackerRetentionPeriod = DefaultTrackerRetentionPeriod,
      engineMode = EngineMode.Stable,
      enableAppendOnlySchema = false,
      enableMutableContractStateCache = false,
      extra = extra,
    )

  def ownerWithoutExtras(name: String, args: collection.Seq[String]): ResourceOwner[Config[Unit]] =
    owner(name, _ => (), (), args)

  def owner[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: collection.Seq[String],
  ): ResourceOwner[Config[Extra]] =
    parse(name, extraOptions, defaultExtra, args)
      .fold[ResourceOwner[Config[Extra]]](ResourceOwner.failed(new ConfigParseException))(
        ResourceOwner.successful
      )

  def parse[Extra](
      name: String,
      extraOptions: OptionParser[Config[Extra]] => Unit,
      defaultExtra: Extra,
      args: collection.Seq[String],
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
    val parser: OptionParser[Config[Extra]] =
      new OptionParser[Config[Extra]](name) {
        head(s"$name as a service")

        cmd("dump-index-metadata")
          .text(
            "Connect to the index db. Print ledger id, ledger end and integration API version and quit."
          )
          .children {
            arg[String]("  <jdbc-url>...")
              .minOccurs(1)
              .unbounded()
              .text("The JDBC URL to connect to an index database")
              .action((jdbcUrl, config) =>
                config.copy(mode = config.mode match {
                  case Mode.Run =>
                    Mode.DumpIndexMetadata(Vector(jdbcUrl))
                  case Mode.DumpIndexMetadata(jdbcUrls) =>
                    Mode.DumpIndexMetadata(jdbcUrls :+ jdbcUrl)
                })
              )
          }

        arg[File]("<archive>...")
          .optional()
          .unbounded()
          .text(
            "DAR files to load. Scenarios are ignored. The server starts with an empty ledger by default."
          )
          .action((file, config) => config.copy(archiveFiles = config.archiveFiles :+ file.toPath))

        help("help").text("Print this help page.")

        opt[Map[String, String]]("participant")
          .unbounded()
          .text(
            "The configuration of a participant. Comma-separated pairs in the form key=value, with mandatory keys: [participant-id, port] and optional keys [" +
              "address, " +
              "port-file, " +
              "server-jdbc-url, " +
              "api-server-connection-pool-size" +
              "api-server-connection-timeout" +
              "max-commands-in-flight, " +
              "management-service-timeout, " +
              "run-mode, " +
              "shard-name, " +
              "indexer-connection-pool-size, " +
              "indexer-connection-timeout, " +
              "indexer-max-input-buffer-size, " +
              "indexer-input-mapping-parallelism, " +
              "indexer-ingestion-parallelism, " +
              "indexer-submission-batch-size, " +
              "indexer-tailing-rate-limit-per-second, " +
              "indexer-batch-within-millis, " +
              "indexer-enable-compression, " +
              "contract-state-cache-max-size, " +
              "contract-key-state-cache-max-size, " +
              "]"
          )
          .action((kv, config) => {
            val participantId =
              ParticipantId.assertFromString(kv("participant-id"))
            val port = Port(kv("port").toInt)
            val address = kv.get("address")
            val portFile = kv.get("port-file").map(new File(_).toPath)
            val runMode: ParticipantRunMode = kv.get("run-mode") match {
              case None => ParticipantRunMode.Combined
              case Some("combined") => ParticipantRunMode.Combined
              case Some("indexer") => ParticipantRunMode.Indexer
              case Some("ledger-api-server") =>
                ParticipantRunMode.LedgerApiServer
              case Some(unknownMode) =>
                throw new RuntimeException(
                  s"$unknownMode is not a valid run mode. Valid modes are: combined, indexer, ledger-api-server. Default mode is combined."
                )
            }
            val jdbcUrlFromEnv =
              kv.get("server-jdbc-url-env").flatMap(getEnvVar(_))
            val jdbcUrl =
              kv.getOrElse(
                "server-jdbc-url",
                jdbcUrlFromEnv.getOrElse(ParticipantConfig.defaultIndexJdbcUrl(participantId)),
              )
            val apiServerConnectionPoolSize = kv
              .get("api-server-connection-pool-size")
              .map(_.toInt)
              .getOrElse(ParticipantConfig.DefaultApiServerDatabaseConnectionPoolSize)

            val apiServerConnectionTimeout = kv
              .get("api-server-connection-timeout")
              .map(Duration.parse)
              .getOrElse(ParticipantConfig.DefaultApiServerDatabaseConnectionTimeout)
            val indexerConnectionPoolSize = kv
              .get("indexer-connection-pool-size")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultDatabaseConnectionPoolSize)
            val indexerConnectionTimeout = kv
              .get("indexer-connection-timeout")
              .map(Duration.parse)
              .getOrElse(ParticipantConfig.DefaultApiServerDatabaseConnectionTimeout)
            val indexerInputMappingParallelism = kv
              .get("indexer-input-mapping-parallelism")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultInputMappingParallelism)
            val indexerMaxInputBufferSize = kv
              .get("indexer-max-input-buffer-size")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultMaxInputBufferSize)
            val indexerBatchingParallelism = kv
              .get("indexer-batching-parallelism")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultBatchingParallelism)
            val indexerIngestionParallelism = kv
              .get("indexer-ingestion-parallelism")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultIngestionParallelism)
            val indexerSubmissionBatchSize = kv
              .get("indexer-submission-batch-size")
              .map(_.toLong)
              .getOrElse(ParticipantIndexerConfig.DefaultSubmissionBatchSize)
            val indexerTailingRateLimitPerSecond = kv
              .get("indexer-tailing-rate-limit-per-second")
              .map(_.toInt)
              .getOrElse(ParticipantIndexerConfig.DefaultTailingRateLimitPerSecond)
            val indexerBatchWithinMillis = kv
              .get("indexer-batch-within-millis")
              .map(_.toLong)
              .getOrElse(ParticipantIndexerConfig.DefaultBatchWithinMillis)
            val indexerEnableCompression = kv
              .get("indexer-enable-compression")
              .map(_.toBoolean)
              .getOrElse(ParticipantIndexerConfig.DefaultEnableCompression)

            val maxCommandsInFlight =
              kv.get("max-commands-in-flight").map(_.toInt)
            val managementServiceTimeout = kv
              .get("management-service-timeout")
              .map(Duration.parse)
              .getOrElse(ParticipantConfig.DefaultManagementServiceTimeout)
            val shardName = kv.get("shard-name")
            val maxContractStateCacheSize = kv
              .get("contract-state-cache-max-size")
              .map(_.toLong)
              .getOrElse(ParticipantConfig.DefaultMaxContractStateCacheSize)
            val maxContractKeyStateCacheSize = kv
              .get("contract-key-state-cache-max-size")
              .map(_.toLong)
              .getOrElse(ParticipantConfig.DefaultMaxContractKeyStateCacheSize)
            val partConfig = ParticipantConfig(
              runMode,
              participantId,
              shardName,
              address,
              port,
              portFile,
              jdbcUrl,
              indexerConfig = ParticipantIndexerConfig(
                databaseConnectionPoolSize = indexerConnectionPoolSize,
                databaseConnectionTimeout =
                  FiniteDuration(indexerConnectionTimeout.toMillis, TimeUnit.MILLISECONDS),
                allowExistingSchema = false,
                maxInputBufferSize = indexerMaxInputBufferSize,
                inputMappingParallelism = indexerInputMappingParallelism,
                batchingParallelism = indexerBatchingParallelism,
                ingestionParallelism = indexerIngestionParallelism,
                submissionBatchSize = indexerSubmissionBatchSize,
                tailingRateLimitPerSecond = indexerTailingRateLimitPerSecond,
                batchWithinMillis = indexerBatchWithinMillis,
                enableCompression = indexerEnableCompression,
              ),
              apiServerDatabaseConnectionPoolSize = apiServerConnectionPoolSize,
              apiServerDatabaseConnectionTimeout = apiServerConnectionTimeout,
              maxCommandsInFlight = maxCommandsInFlight,
              managementServiceTimeout = managementServiceTimeout,
              maxContractStateCacheSize = maxContractStateCacheSize,
              maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
            )
            config.copy(participants = config.participants :+ partConfig)
          })

        opt[String]("ledger-id")
          .optional()
          .text(
            "The ID of the ledger. This must be the same each time the ledger is started. Defaults to a random UUID."
          )
          .action((ledgerId, config) => config.copy(ledgerId = ledgerId))

        opt[String]("pem")
          .optional()
          .text("TLS: The pem file to be used as the private key.")
          .action((path, config) =>
            config.withTlsConfig(c => c.copy(keyFile = Some(new File(path))))
          )
        opt[String]("crt")
          .optional()
          .text(
            "TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set."
          )
          .action((path, config) =>
            config.withTlsConfig(c => c.copy(keyCertChainFile = Some(new File(path))))
          )
        opt[String]("cacrt")
          .optional()
          .text("TLS: The crt file to be used as the trusted root CA.")
          .action((path, config) =>
            config.withTlsConfig(c => c.copy(trustCertCollectionFile = Some(new File(path))))
          )
        opt[Boolean]("cert-revocation-checking")
          .optional()
          .text(
            "TLS: enable/disable certificate revocation checks with the OCSP. Disabled by default."
          )
          .action((checksEnabled, config) =>
            config.withTlsConfig(c => c.copy(enableCertRevocationChecking = checksEnabled))
          )
        opt[ClientAuth]("client-auth")
          .optional()
          .text(
            "TLS: The client authentication mode. Must be one of none, optional or require. If TLS is enabled it defaults to require."
          )
          .action((clientAuth, config) =>
            config.withTlsConfig(c => c.copy(clientAuth = clientAuth))
          )

        opt[Duration]("tracker-retention-period")
          .optional()
          .action((value, config) =>
            config.copy(trackerRetentionPeriod = FiniteDuration(value.getSeconds, TimeUnit.SECONDS))
          )
          .text(
            s"How long will the command service keep an active command tracker for a given party. A longer period cuts down on the tracker instantiation cost for a party that seldom acts. A shorter period causes a quick removal of unused trackers. Default is $DefaultTrackerRetentionPeriod."
          )

        opt[Int]("max-inbound-message-size")
          .optional()
          .text(
            s"Max inbound message size in bytes. Defaults to ${Config.DefaultMaxInboundMessageSize}."
          )
          .action((maxInboundMessageSize, config) =>
            config.copy(maxInboundMessageSize = maxInboundMessageSize)
          )

        opt[Int]("events-page-size")
          .optional()
          .text(
            s"Number of events fetched from the index for every round trip when serving streaming calls. Default is ${IndexConfiguration.DefaultEventsPageSize}."
          )
          .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize))

        opt[Long]("max-state-value-cache-size")
          .optional()
          .text(
            s"The maximum size of the cache used to deserialize state values, in MB. By default, nothing is cached."
          )
          .action((maximumStateValueCacheSize, config) =>
            config.copy(stateValueCache =
              config.stateValueCache.copy(maximumWeight = maximumStateValueCacheSize * 1024 * 1024)
            )
          )

        opt[Long]("max-lf-value-translation-cache-entries")
          .optional()
          .text(
            s"The maximum size of the cache used to deserialize Daml-LF values, in number of allowed entries. By default, nothing is cached."
          )
          .action((maximumLfValueTranslationCacheEntries, config) =>
            config.copy(
              lfValueTranslationEventCache = config.lfValueTranslationEventCache
                .copy(maximumSize = maximumLfValueTranslationCacheEntries),
              lfValueTranslationContractCache = config.lfValueTranslationContractCache
                .copy(maximumSize = maximumLfValueTranslationCacheEntries),
            )
          )

        private val seedingMap =
          Map[String, Seeding]("testing-weak" -> Seeding.Weak, "strong" -> Seeding.Strong)

        opt[String]("contract-id-seeding")
          .optional()
          .text(s"""Set the seeding of contract ids. Possible values are ${seedingMap.keys
            .mkString(
              ","
            )}. Default is "strong".""")
          .validate(v =>
            Either.cond(
              seedingMap.contains(v.toLowerCase),
              (),
              s"seeding must be ${seedingMap.keys.mkString(",")}",
            )
          )
          .action((text, config) => config.copy(seeding = seedingMap(text)))
          .hidden()

        opt[MetricsReporter]("metrics-reporter")
          .optional()
          .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")
          .action((reporter, config) => config.copy(metricsReporter = Some(reporter)))

        opt[Duration]("metrics-reporting-interval")
          .optional()
          .text("Set metric reporting interval.")
          .action((interval, config) => config.copy(metricsReportingInterval = interval))

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
                ". Use the optional 'shard-name' key when specifying horizontally scaled participants.",
              )
            )
          else
            success
        })

        opt[Unit]("early-access")
          .optional()
          .action((_, c) => c.copy(engineMode = EngineMode.EarlyAccess))
          .text(
            "Enable preview version of the next Daml-LF language. Should not be used in production."
          )

        opt[Unit]("daml-lf-dev-mode-unsafe")
          .optional()
          .hidden()
          .action((_, c) => c.copy(engineMode = EngineMode.Dev))
          .text(
            "Enable the development version of the Daml-LF language. Highly unstable. Should not be used in production."
          )

        // TODO append-only: remove after removing support for the current (mutating) schema
        opt[Unit]("index-append-only-schema-unsafe")
          .optional()
          .hidden()
          .text(
            s"Use the append-only index database with parallel ingestion. Highly unstable. Should not be used in production."
          )
          .action((_, config) => config.copy(enableAppendOnlySchema = true))

        opt[Unit]("mutable-contract-state-cache-unsafe")
          .optional()
          .hidden()
          .text(
            "Experimental contract state cache for command execution. Should not be used in production."
          )
          .action((_, config) => config.copy(enableMutableContractStateCache = true))
      }
    extraOptions(parser)
    parser
  }

  sealed abstract class EngineMode extends Product with Serializable

  object EngineMode {
    final case object Stable extends EngineMode
    final case object EarlyAccess extends EngineMode
    final case object Dev extends EngineMode
  }

}
