// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.caching
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.VersionRange
import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.Readers._
import com.daml.platform.configuration.{CommandConfiguration, IndexConfiguration}
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import scopt.OptionParser

import java.io.File
import java.net.URL
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final case class Config[Extra](
    mode: Mode,
    ledgerId: String,
    archiveFiles: Seq[Path],
    commandConfig: CommandConfiguration,
    tlsConfig: Option[TlsConfiguration],
    participants: Seq[ParticipantConfig],
    maxInboundMessageSize: Int,
    configurationLoadTimeout: Duration,
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    stateValueCache: caching.WeightedCache.Configuration,
    lfValueTranslationEventCache: caching.SizedCache.Configuration,
    lfValueTranslationContractCache: caching.SizedCache.Configuration,
    seeding: Seeding,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    allowedLanguageVersions: VersionRange[LanguageVersion],
    enableAppendOnlySchema: Boolean, // TODO append-only: remove after removing support for the current (mutating) schema
    enableMutableContractStateCache: Boolean,
    enableInMemoryFanOutForLedgerApi: Boolean,
    enableHa: Boolean, // TODO ha: remove after stable
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
      commandConfig = CommandConfiguration.default,
      tlsConfig = None,
      participants = Vector.empty,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      configurationLoadTimeout = Duration.ofSeconds(10),
      eventsPageSize = IndexConfiguration.DefaultEventsPageSize,
      eventsProcessingParallelism = IndexConfiguration.DefaultEventsProcessingParallelism,
      stateValueCache = caching.WeightedCache.Configuration.none,
      lfValueTranslationEventCache = caching.SizedCache.Configuration.none,
      lfValueTranslationContractCache = caching.SizedCache.Configuration.none,
      seeding = Seeding.Strong,
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      allowedLanguageVersions = LanguageVersion.StableVersions,
      enableAppendOnlySchema = false,
      enableMutableContractStateCache = false,
      enableInMemoryFanOutForLedgerApi = false,
      enableHa = false,
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
            val participantId = Ref.ParticipantId.assertFromString(kv("participant-id"))
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
            val maxTransactionsInMemoryFanOutBufferSize = kv
              .get("ledger-api-transactions-buffer-max-size")
              .map(_.toLong)
              .getOrElse(ParticipantConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize)
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
              managementServiceTimeout = managementServiceTimeout,
              maxContractStateCacheSize = maxContractStateCacheSize,
              maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
              maxTransactionsInMemoryFanOutBufferSize = maxTransactionsInMemoryFanOutBufferSize,
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
          .text(
            "TLS: The pem file to be used as the private key. Use '.enc' filename suffix if the pem file is encrypted."
          )
          .action((path, config) =>
            config.withTlsConfig(c => c.copy(keyFile = Some(new File(path))))
          )

        opt[String]("secrets-url")
          .optional()
          .text(
            "TLS: URL of a secrets service that provide parameters needed to decrypt the private key. Required when private key is encrypted (indicated by '.enc' filename suffix)."
          )
          .action((url, config) =>
            config.withTlsConfig(c => c.copy(secretsUrl = Some(new URL(url))))
          )

        checkConfig(c =>
          c.tlsConfig.fold(success) { tlsConfig =>
            if (
              tlsConfig.keyFile.isDefined
              && tlsConfig.keyFile.get.getName.endsWith(".enc")
              && tlsConfig.secretsUrl.isEmpty
            ) {
              failure(
                "You need to provide a secrets server URL if the server's private key is an encrypted file."
              )
            } else {
              success
            }
          }
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

        opt[Int]("max-commands-in-flight")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(maxCommandsInFlight = value))
          )
          .text(
            "Maximum number of submitted commands waiting for completion for each party (only applied when using the CommandService). Overflowing this threshold will cause back-pressure, signaled by a RESOURCE_EXHAUSTED error code. Default is 256."
          )

        opt[Int]("max-parallel-submissions")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(maxParallelSubmissions = value))
          )
          .text(
            "Maximum number of successfully interpreted commands waiting to be sequenced (applied only when running sandbox-classic). The threshold is shared across all parties. Overflowing it will cause back-pressure, signaled by a RESOURCE_EXHAUSTED error code. Default is 512."
          )

        opt[Int]("input-buffer-size")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(inputBufferSize = value))
          )
          .text(
            "The maximum number of commands waiting to be submitted for each party. Overflowing this threshold will cause back-pressure, signaled by a RESOURCE_EXHAUSTED error code. Default is 512."
          )

        opt[Duration]("tracker-retention-period")
          .optional()
          .action((value, config) =>
            config.copy(
              commandConfig = config.commandConfig.copy(
                trackerRetentionPeriod = FiniteDuration(value.getSeconds, TimeUnit.SECONDS)
              )
            )
          )
          .text(
            "The duration that the command service will keep an active command tracker for a given set of parties." +
              " A longer period cuts down on the tracker instantiation cost for a party that seldom acts." +
              " A shorter period causes a quick removal of unused trackers." +
              s" Default is ${CommandConfiguration.DefaultTrackerRetentionPeriod}."
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
          .validate { pageSize =>
            if (pageSize > 0) Right(())
            else Left("events-page-size should be strictly positive")
          }
          .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize))

        opt[Int]("buffers-prefetching-parallelism")
          .optional()
          .text(
            s"Number of events fetched/decoded in parallel for populating the Ledger API internal buffers. Default is ${IndexConfiguration.DefaultEventsProcessingParallelism}."
          )
          .validate { buffersPrefetchingParallelism =>
            if (buffersPrefetchingParallelism > 0) Right(())
            else Left("buffers-prefetching-parallelism should be strictly positive")
          }
          .action((eventsProcessingParallelism, config) =>
            config.copy(eventsProcessingParallelism = eventsProcessingParallelism)
          )

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
          .action((_, c) => c.copy(allowedLanguageVersions = LanguageVersion.EarlyAccessVersions))
          .text(
            "Enable preview version of the next Daml-LF language. Should not be used in production."
          )

        opt[Unit]("daml-lf-dev-mode-unsafe")
          .optional()
          .hidden()
          .action((_, c) => c.copy(allowedLanguageVersions = LanguageVersion.DevVersions))
          .text(
            "Enable the development version of the Daml-LF language. Highly unstable. Should not be used in production."
          )

        opt[Unit]("daml-lf-min-version-1.14-unsafe")
          .optional()
          .hidden()
          .action((_, c) =>
            c.copy(allowedLanguageVersions =
              c.allowedLanguageVersions.copy(min = LanguageVersion.v1_14)
            )
          )
          .text(
            "Set minimum LF version for unstable packages to 1.14. Should not be used in production."
          )

        // TODO append-only: remove after removing support for the current (mutating) schema
        opt[Unit]("index-append-only-schema")
          .optional()
          .hidden()
          .text(
            s"Use the append-only index database with parallel ingestion."
          )
          .action((_, config) => config.copy(enableAppendOnlySchema = true))

        opt[Unit]("mutable-contract-state-cache")
          .optional()
          .hidden()
          .text(
            "Contract state cache for command execution. Must be enabled in conjunction with index-append-only-schema."
          )
          .action((_, config) => config.copy(enableMutableContractStateCache = true))

        opt[Unit]("buffered-ledger-api-streams-unsafe")
          .optional()
          .hidden()
          .text(
            "Experimental buffer for Ledger API streaming queries. Must be enabled in conjunction with index-append-only-schema and mutable-contract-state-cache. Should not be used in production."
          )
          .action((_, config) => config.copy(enableInMemoryFanOutForLedgerApi = true))

        checkConfig(config =>
          if (config.enableMutableContractStateCache && !config.enableAppendOnlySchema)
            failure(
              "mutable-contract-state-cache must be enabled in conjunction with index-append-only-schema."
            )
          else if (
            config.enableInMemoryFanOutForLedgerApi && !(config.enableMutableContractStateCache && config.enableAppendOnlySchema)
          )
            failure(
              "buffered-ledger-api-streams-unsafe must be enabled in conjunction with index-append-only-schema and mutable-contract-state-cache."
            )
          else success
        )

        // TODO ha: remove after stable
        opt[Unit]("index-ha-unsafe")
          .optional()
          .hidden()
          .text(
            s"Use the experimental High Availability feature with the indexer. Should not be used in production."
          )
          .action((_, config) => config.copy(enableHa = true))
      }
    extraOptions(parser)
    parser
  }
}
