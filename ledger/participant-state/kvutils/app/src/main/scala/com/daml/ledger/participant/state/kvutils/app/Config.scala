// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.caching
import com.daml.jwt.JwtVerifierConfigurationCli
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.VersionRange
import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.Readers._
import com.daml.platform.configuration.{CommandConfiguration, IndexConfiguration}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import scopt.OptionParser

import java.io.File
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final case class Config[Extra](
    allowedLanguageVersions: VersionRange[LanguageVersion],
    authService: AuthService,
    acsContractFetchingParallelism: Int,
    acsGlobalParallelism: Int,
    acsIdFetchingParallelism: Int,
    acsIdPageSize: Int,
    acsIdQueueLimit: Int,
    configurationLoadTimeout: Duration,
    commandConfig: CommandConfiguration,
    enableInMemoryFanOutForLedgerApi: Boolean,
    enableSelfServiceErrorCodes: Boolean,
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    extra: Extra,
    ledgerId: String,
    lfValueTranslationContractCache: caching.SizedCache.Configuration,
    lfValueTranslationEventCache: caching.SizedCache.Configuration,
    maxDeduplicationDuration: Option[Duration],
    maxInboundMessageSize: Int,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    mode: Mode,
    participants: Seq[ParticipantConfig],
    profileDir: Option[Path],
    seeding: Seeding,
    stackTraces: Boolean,
    stateValueCache: caching.WeightedCache.Configuration,
    timeProviderType: TimeProviderType,
    tlsConfig: Option[TlsConfiguration],
    userManagementConfig: UserManagementConfig,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))

  def withUserManagementConfig(
      modify: UserManagementConfig => UserManagementConfig
  ): Config[Extra] =
    copy(userManagementConfig = modify(userManagementConfig))
}

object Config {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024

  def createDefault[Extra](extra: Extra): Config[Extra] =
    Config(
      allowedLanguageVersions = LanguageVersion.StableVersions,
      authService = AuthServiceWildcard,
      acsContractFetchingParallelism = IndexConfiguration.DefaultAcsContractFetchingParallelism,
      acsGlobalParallelism = IndexConfiguration.DefaultAcsGlobalParallelism,
      acsIdFetchingParallelism = IndexConfiguration.DefaultAcsIdFetchingParallelism,
      acsIdPageSize = IndexConfiguration.DefaultAcsIdPageSize,
      acsIdQueueLimit = IndexConfiguration.DefaultAcsIdQueueLimit,
      configurationLoadTimeout = Duration.ofSeconds(10),
      commandConfig = CommandConfiguration.default,
      enableInMemoryFanOutForLedgerApi = false,
      enableSelfServiceErrorCodes = true,
      eventsPageSize = IndexConfiguration.DefaultEventsPageSize,
      eventsProcessingParallelism = IndexConfiguration.DefaultEventsProcessingParallelism,
      extra = extra,
      ledgerId = UUID.randomUUID().toString,
      lfValueTranslationContractCache = caching.SizedCache.Configuration.none,
      lfValueTranslationEventCache = caching.SizedCache.Configuration.none,
      maxDeduplicationDuration = None,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      mode = Mode.Run,
      participants = Vector.empty,
      profileDir = None,
      seeding = Seeding.Strong,
      stackTraces = false,
      stateValueCache = caching.WeightedCache.Configuration.none,
      timeProviderType = TimeProviderType.WallClock,
      tlsConfig = None,
      userManagementConfig = UserManagementConfig.default(enabled = false),
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
              "indexer-batching-parallelism, " +
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

        opt[String]("tls-secrets-url")
          .optional()
          .text(
            "TLS: URL of a secrets service that provide parameters needed to decrypt the private key. Required when private key is encrypted (indicated by '.enc' filename suffix)."
          )
          .action((url, config) =>
            config.withTlsConfig(c => c.copy(secretsUrl = Some(SecretsUrl.fromString(url))))
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

        opt[TlsVersion]("min-tls-version")
          .optional()
          .text(
            "TLS: Indicates the minimum TLS version to enable. If specified must be either '1.2' or '1.3'."
          )
          .action((tlsVersion, config) =>
            config.withTlsConfig(c => c.copy(minimumServerProtocolVersion = Some(tlsVersion)))
          )

        opt[Int]("max-commands-in-flight")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(maxCommandsInFlight = value))
          )
          .text(
            s"Maximum number of submitted commands for which the CommandService is waiting to be completed in parallel, for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause new submissions to wait in the queue before being submitted. Default is ${CommandConfiguration.default.maxCommandsInFlight}."
          )

        opt[Int]("input-buffer-size")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(inputBufferSize = value))
          )
          .text(
            s"Maximum number of commands waiting to be submitted for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause the server to signal backpressure using the ``RESOURCE_EXHAUSTED`` gRPC status code. Default is ${CommandConfiguration.default.inputBufferSize}."
          )

        opt[Duration]("tracker-retention-period")
          .optional()
          .action((value, config) =>
            config.copy(commandConfig = config.commandConfig.copy(trackerRetentionPeriod = value))
          )
          .text(
            "The duration that the command service will keep an active command tracker for a given set of parties." +
              " A longer period cuts down on the tracker instantiation cost for a party that seldom acts." +
              " A shorter period causes a quick removal of unused trackers." +
              s" Default is ${CommandConfiguration.DefaultTrackerRetentionPeriod}."
          )

        opt[Duration]("max-deduplication-duration")
          .optional()
          .hidden()
          .action((maxDeduplicationDuration, config) =>
            config
              .copy(maxDeduplicationDuration = Some(maxDeduplicationDuration))
          )
          .text(
            "Maximum command deduplication duration."
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

        opt[Int]("acs-id-page-size")
          .optional()
          .text(
            s"Number of contract ids fetched from the index for every round trip when serving ACS calls. Default is ${IndexConfiguration.DefaultAcsIdPageSize}."
          )
          .validate { acsIdPageSize =>
            if (acsIdPageSize > 0) Right(())
            else Left("acs-id-page-size should be strictly positive")
          }
          .action((acsIdPageSize, config) => config.copy(acsIdPageSize = acsIdPageSize))

        opt[Int]("acs-id-fetching-parallelism")
          .optional()
          .text(
            s"Number of contract id pages fetched in parallel when serving ACS calls. Default is ${IndexConfiguration.DefaultAcsIdFetchingParallelism}."
          )
          .validate { acsIdFetchingParallelism =>
            if (acsIdFetchingParallelism > 0) Right(())
            else Left("acs-id-fetching-parallelism should be strictly positive")
          }
          .action((acsIdFetchingParallelism, config) =>
            config.copy(acsIdFetchingParallelism = acsIdFetchingParallelism)
          )

        opt[Int]("acs-contract-fetching-parallelism")
          .optional()
          .text(
            s"Number of event pages fetched in parallel when serving ACS calls. Default is ${IndexConfiguration.DefaultAcsContractFetchingParallelism}."
          )
          .validate { acsContractFetchingParallelism =>
            if (acsContractFetchingParallelism > 0) Right(())
            else Left("acs-contract-fetching-parallelism should be strictly positive")
          }
          .action((acsContractFetchingParallelism, config) =>
            config.copy(acsContractFetchingParallelism = acsContractFetchingParallelism)
          )

        opt[Int]("acs-global-parallelism-limit")
          .optional()
          .text(
            s"Maximum number of concurrent ACS queries to the index database. Default is ${IndexConfiguration.DefaultAcsGlobalParallelism}."
          )
          .action((acsGlobalParallelism, config) =>
            config.copy(acsGlobalParallelism = acsGlobalParallelism)
          )

        opt[Int]("acs-id-queue-limit")
          .optional()
          .text(
            s"Maximum number of contract ids queued for fetching. Default is ${IndexConfiguration.DefaultAcsIdQueueLimit}."
          )
          .action((acsIdQueueLimit, config) => config.copy(acsIdQueueLimit = acsIdQueueLimit))

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

        // TODO append-only: remove
        opt[Unit]("index-append-only-schema")
          .optional()
          .text("Legacy flag with no effect")
          .action((_, config) => config)

        // TODO remove
        opt[Unit]("mutable-contract-state-cache")
          .optional()
          .hidden()
          .text("Legacy flag with no effect")
          .action((_, config) => config)

        opt[Unit]("buffered-ledger-api-streams-unsafe")
          .optional()
          .hidden()
          .text(
            "Experimental buffer for Ledger API streaming queries. Should not be used in production."
          )
          .action((_, config) => config.copy(enableInMemoryFanOutForLedgerApi = true))

        opt[Unit]("use-pre-1.18-error-codes")
          .optional()
          .text(
            "Enables gRPC error code compatibility mode to the pre-1.18 behaviour. This option is deprecated and will be removed in a future release."
          )
          .action((_, config: Config[Extra]) => config.copy(enableSelfServiceErrorCodes = false))

        opt[Boolean]("enable-user-management")
          .optional()
          .text(
            "Whether to enable participant user management."
          )
          .action((enabled, config: Config[Extra]) =>
            config.withUserManagementConfig(_.copy(enabled = enabled))
          )

        opt[Int]("user-management-cache-expiry")
          .optional()
          .text(
            s"Defaults to ${UserManagementConfig.DefaultCacheExpiryAfterWriteInSeconds} seconds. " +
              "Used to set expiry time for user management cache. " +
              "Also determines the maximum delay for propagating user management state changes which is double its value."
          )
          .action((value, config: Config[Extra]) =>
            config.withUserManagementConfig(_.copy(cacheExpiryAfterWriteInSeconds = value))
          )

        opt[Int]("user-management-max-cache-size")
          .optional()
          .text(
            s"Defaults to ${UserManagementConfig.DefaultMaxCacheSize} entries. " +
              "Determines the maximum in-memory cache size for user management state."
          )
          .action((value, config: Config[Extra]) =>
            config.withUserManagementConfig(_.copy(maxCacheSize = value))
          )

        opt[Int]("max-users-page-size")
          .optional()
          .text(
            s"Maximum number of users that the server can return in a single response. " +
              s"Defaults to ${UserManagementConfig.DefaultMaxUsersPageSize} entries."
          )
          .action((value, config: Config[Extra]) =>
            config.withUserManagementConfig(_.copy(maxUsersPageSize = value))
          )
        opt[Unit]('s', "static-time")
          .optional()
          .hidden() // Only available for testing purposes
          .action((_, c) => c.copy(timeProviderType = TimeProviderType.Static))
          .text("Use static time. When not specified, wall-clock-time is used.")

        opt[File]("profile-dir")
          .optional()
          .action((dir, config) => config.copy(profileDir = Some(dir.toPath)))
          .text("Enable profiling and write the profiles into the given directory.")

        opt[Boolean]("stack-traces")
          .hidden()
          .optional()
          .action((enabled, config) => config.copy(stackTraces = enabled))
          .text(
            "Enable/disable stack traces. Default is to disable them. " +
              "Enabling stack traces may have a significant performance impact."
          )

        JwtVerifierConfigurationCli.parse(this)((v, c) => c.copy(authService = AuthServiceJWT(v)))
      }
    extraOptions(parser)
    parser
  }
}
