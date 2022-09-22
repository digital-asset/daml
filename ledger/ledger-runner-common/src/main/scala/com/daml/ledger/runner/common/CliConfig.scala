// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration}
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.{AuthServiceConfig, AuthServiceConfigCli}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.config.ParticipantConfig
import com.daml.platform.configuration.Readers._
import com.daml.platform.configuration.{CommandConfiguration, IndexServiceConfig}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import scopt.OParser

import java.io.File
import java.time.Duration
import java.util.UUID
import scala.jdk.DurationConverters.JavaDurationOps

final case class CliConfig[Extra](
    engineConfig: EngineConfig,
    authService: AuthServiceConfig,
    acsContractFetchingParallelism: Int,
    acsGlobalParallelism: Int,
    acsIdFetchingParallelism: Int,
    acsIdPageSize: Int,
    configurationLoadTimeout: Duration,
    commandConfig: CommandConfiguration,
    eventsPageSize: Int,
    bufferedStreamsPageSize: Int,
    eventsProcessingParallelism: Int,
    extra: Extra,
    ledgerId: String,
    maxDeduplicationDuration: Option[Duration],
    maxInboundMessageSize: Int,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    mode: Mode,
    participants: Seq[CliParticipantConfig],
    seeding: Seeding,
    timeProviderType: TimeProviderType,
    tlsConfig: Option[TlsConfiguration],
    userManagementConfig: UserManagementConfig,
    maxTransactionsInMemoryFanOutBufferSize: Int,
    configFiles: Seq[File] = Seq(),
    configMap: Map[String, String] = Map(),
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): CliConfig[Extra] =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))

  def withUserManagementConfig(
      modify: UserManagementConfig => UserManagementConfig
  ): CliConfig[Extra] =
    copy(userManagementConfig = modify(userManagementConfig))
}

object CliConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024

  def createDefault[Extra](extra: Extra): CliConfig[Extra] =
    CliConfig(
      engineConfig = EngineConfig(
        allowedLanguageVersions = LanguageVersion.StableVersions,
        profileDir = None,
        stackTraceMode = false,
        forbidV0ContractId = true,
      ),
      authService = AuthServiceConfig.Wildcard,
      acsContractFetchingParallelism = IndexServiceConfig.DefaultAcsContractFetchingParallelism,
      acsGlobalParallelism = IndexServiceConfig.DefaultAcsGlobalParallelism,
      acsIdFetchingParallelism = IndexServiceConfig.DefaultAcsIdFetchingParallelism,
      acsIdPageSize = IndexServiceConfig.DefaultAcsIdPageSize,
      configurationLoadTimeout = Duration.ofSeconds(10),
      commandConfig = CommandConfiguration.Default,
      eventsPageSize = IndexServiceConfig.DefaultEventsPageSize,
      bufferedStreamsPageSize = IndexServiceConfig.DefaultBufferedStreamsPageSize,
      eventsProcessingParallelism = IndexServiceConfig.DefaultEventsProcessingParallelism,
      extra = extra,
      ledgerId = UUID.randomUUID().toString,
      maxDeduplicationDuration = None,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      mode = Mode.Run,
      participants = Vector.empty,
      seeding = Seeding.Strong,
      timeProviderType = TimeProviderType.WallClock,
      tlsConfig = None,
      userManagementConfig = UserManagementConfig.default(enabled = false),
      maxTransactionsInMemoryFanOutBufferSize =
        IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    )

  private def checkNoEmptyParticipant[Extra](config: CliConfig[Extra]): Either[String, Unit] =
    if (config.mode == Mode.RunLegacyCliConfig && config.participants.isEmpty)
      OParser.builder[CliConfig[Extra]].failure("No --participant provided to run")
    else
      OParser.builder[CliConfig[Extra]].success

  private def checkFileCanBeRead[Extra](config: CliConfig[Extra]): Either[String, Unit] = {
    val fileErrors = config.configFiles.collect {
      case file if !file.canRead => s"Could not read file ${file.getName}"
    }
    if (fileErrors.nonEmpty) {
      OParser.builder[CliConfig[Extra]].failure(fileErrors.mkString(", "))
    } else
      OParser.builder[CliConfig[Extra]].success
  }

  def parse[Extra](
      name: String,
      extraOptions: OParser[_, CliConfig[Extra]],
      defaultExtra: Extra,
      args: collection.Seq[String],
      getEnvVar: String => Option[String] = sys.env.get(_),
  ): Option[CliConfig[Extra]] = {
    val builder = OParser.builder[CliConfig[Extra]]
    val parser: OParser[_, CliConfig[Extra]] = OParser.sequence(
      builder.head(s"$name as a service"),
      builder.help("help").text("Print this help page."),
      commandRunLegacy(name, getEnvVar, extraOptions),
      commandDumpIndexMetadata,
      commandRunHocon(name),
      commandConvertConfig(getEnvVar, extraOptions),
      builder.checkConfig(checkNoEmptyParticipant),
    )

    OParser.parse[CliConfig[Extra]](
      parser,
      args,
      createDefault(defaultExtra),
    )
  }

  private def configKeyValueOption[Extra] =
    OParser
      .builder[CliConfig[Extra]]
      .opt[Map[String, String]]('C', "config key-value's")
      .text(
        "Set configuration key value pairs directly. Can be useful for providing simple short config info."
      )
      .valueName("<key1>=<value1>,<key2>=<value2>,...")
      .unbounded()
      .action { (map, cli) =>
        cli.copy(configMap = map ++ cli.configMap)
      }

  private def configFilesOption[Extra] =
    OParser
      .builder[CliConfig[Extra]]
      .opt[Seq[File]]('c', "config")
      .text(
        "Set configuration file(s). If several configuration files assign values to the same key, the last value is taken."
      )
      .valueName("<file1>,<file2>,...")
      .unbounded()
      .action((files, cli) => cli.copy(configFiles = cli.configFiles ++ files))

  private def commandRunHocon[Extra](name: String): OParser[_, CliConfig[Extra]] = {
    val builder = OParser.builder[CliConfig[Extra]]
    OParser.sequence(
      builder
        .cmd("run")
        .text(
          s"Run $name with configuration provided in HOCON files."
        )
        .action((_, config) => config.copy(mode = Mode.Run))
        .children(
          OParser.sequence(
            configKeyValueOption,
            configFilesOption,
          )
        ),
      builder.checkConfig(checkFileCanBeRead),
    )
  }

  private def commandRunLegacy[Extra](
      name: String,
      getEnvVar: String => Option[String],
      extraOptions: OParser[_, CliConfig[Extra]],
  ): OParser[_, CliConfig[Extra]] =
    OParser
      .builder[CliConfig[Extra]]
      .cmd("run-legacy-cli-config")
      .text(
        s"Run $name in a legacy mode with cli-driven configuration."
      )
      .action((_, config) => config.copy(mode = Mode.RunLegacyCliConfig))
      .children(legacyCommand(getEnvVar, extraOptions))

  private def commandConvertConfig[Extra](
      getEnvVar: String => Option[String],
      extraOptions: OParser[_, CliConfig[Extra]],
  ): OParser[_, CliConfig[Extra]] =
    OParser
      .builder[CliConfig[Extra]]
      .cmd("convert-config")
      .text(
        "Converts configuration provided as legacy CLI options into HOCON file."
      )
      .action((_, config) => config.copy(mode = Mode.ConvertConfig))
      .children(legacyCommand(getEnvVar, extraOptions))

  def legacyCommand[Extra](
      getEnvVar: String => Option[String],
      extraOptions: OParser[_, CliConfig[Extra]],
  ): OParser[_, CliConfig[Extra]] =
    OParser.sequence(parser(getEnvVar), extraOptions)

  private def commandDumpIndexMetadata[Extra]: OParser[_, CliConfig[Extra]] = {
    val builder = OParser.builder[CliConfig[Extra]]
    builder
      .cmd("dump-index-metadata")
      .text(
        "Connect to the index db. Print ledger id, ledger end and integration API version and quit."
      )
      .children {
        builder
          .arg[String]("  <jdbc-url>...")
          .minOccurs(1)
          .unbounded()
          .text("The JDBC URL to connect to an index database")
          .action((jdbcUrl, config) =>
            config.copy(mode = config.mode match {
              case Mode.DumpIndexMetadata(jdbcUrls) =>
                Mode.DumpIndexMetadata(jdbcUrls :+ jdbcUrl)
              case _ =>
                Mode.DumpIndexMetadata(Vector(jdbcUrl))
            })
          )
      }
  }

  private def parser[Extra](
      getEnvVar: String => Option[String]
  ): OParser[_, CliConfig[Extra]] = {
    val builder = OParser.builder[CliConfig[Extra]]
    import builder._
    val seedingMap =
      Map[String, Seeding]("testing-weak" -> Seeding.Weak, "strong" -> Seeding.Strong)
    OParser.sequence(
      configKeyValueOption,
      configFilesOption,
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
          val jdbcUrlFromEnv =
            kv.get("server-jdbc-url-env").flatMap(getEnvVar(_))
          val jdbcUrl =
            kv.getOrElse(
              "server-jdbc-url",
              jdbcUrlFromEnv
                .getOrElse(ParticipantConfig.defaultIndexJdbcUrl(participantId)),
            )
          val apiServerConnectionPoolSize = kv
            .get("api-server-connection-pool-size")
            .map(_.toInt)
            .getOrElse(CliParticipantConfig.DefaultApiServerDatabaseConnectionPoolSize)

          val apiServerConnectionTimeout = kv
            .get("api-server-connection-timeout")
            .map(Duration.parse)
            .map(_.toScala)
            .getOrElse(CliParticipantConfig.DefaultApiServerDatabaseConnectionTimeout)

          val indexerInputMappingParallelism = kv
            .get("indexer-input-mapping-parallelism")
            .map(_.toInt)
            .getOrElse(IndexerConfig.DefaultInputMappingParallelism)
          val indexerMaxInputBufferSize = kv
            .get("indexer-max-input-buffer-size")
            .map(_.toInt)
            .getOrElse(IndexerConfig.DefaultMaxInputBufferSize)
          val indexerBatchingParallelism = kv
            .get("indexer-batching-parallelism")
            .map(_.toInt)
            .getOrElse(IndexerConfig.DefaultBatchingParallelism)
          val indexerIngestionParallelism = kv
            .get("indexer-ingestion-parallelism")
            .map(_.toInt)
            .getOrElse(IndexerConfig.DefaultIngestionParallelism)
          val indexerSubmissionBatchSize = kv
            .get("indexer-submission-batch-size")
            .map(_.toLong)
            .getOrElse(IndexerConfig.DefaultSubmissionBatchSize)
          val indexerEnableCompression = kv
            .get("indexer-enable-compression")
            .map(_.toBoolean)
            .getOrElse(IndexerConfig.DefaultEnableCompression)
          val managementServiceTimeout = kv
            .get("management-service-timeout")
            .map(Duration.parse)
            .map(_.toScala)
            .getOrElse(CliParticipantConfig.DefaultManagementServiceTimeout)
          val maxContractStateCacheSize = kv
            .get("contract-state-cache-max-size")
            .map(_.toLong)
            .getOrElse(IndexServiceConfig.DefaultMaxContractStateCacheSize)
          val maxContractKeyStateCacheSize = kv
            .get("contract-key-state-cache-max-size")
            .map(_.toLong)
            .getOrElse(IndexServiceConfig.DefaultMaxContractKeyStateCacheSize)
          val partConfig = CliParticipantConfig(
            participantId = participantId,
            address = address,
            port = port,
            portFile = portFile,
            serverJdbcUrl = jdbcUrl,
            indexerConfig = IndexerConfig(
              startupMode = IndexerStartupMode.MigrateAndStart(allowExistingSchema = false),
              maxInputBufferSize = indexerMaxInputBufferSize,
              inputMappingParallelism = indexerInputMappingParallelism,
              batchingParallelism = indexerBatchingParallelism,
              ingestionParallelism = indexerIngestionParallelism,
              submissionBatchSize = indexerSubmissionBatchSize,
              enableCompression = indexerEnableCompression,
            ),
            apiServerDatabaseConnectionPoolSize = apiServerConnectionPoolSize,
            apiServerDatabaseConnectionTimeout = apiServerConnectionTimeout,
            managementServiceTimeout = managementServiceTimeout,
            maxContractStateCacheSize = maxContractStateCacheSize,
            maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
          )
          config.copy(participants = config.participants :+ partConfig)
        }),
      opt[String]("ledger-id")
        .optional()
        .text(
          "The ID of the ledger. This must be the same each time the ledger is started. Defaults to a random UUID."
        )
        .action((ledgerId, config) => config.copy(ledgerId = ledgerId)),
      opt[String]("pem")
        .optional()
        .text(
          "TLS: The pem file to be used as the private key. Use '.enc' filename suffix if the pem file is encrypted."
        )
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(privateKeyFile = Some(new File(path))))
        ),
      opt[String]("tls-secrets-url")
        .optional()
        .text(
          "TLS: URL of a secrets service that provide parameters needed to decrypt the private key. Required when private key is encrypted (indicated by '.enc' filename suffix)."
        )
        .action((url, config) =>
          config.withTlsConfig(c => c.copy(secretsUrl = Some(SecretsUrl.fromString(url))))
        ),
      checkConfig(c =>
        c.tlsConfig.fold(success) { tlsConfig =>
          if (
            tlsConfig.privateKeyFile.isDefined
            && tlsConfig.privateKeyFile.get.getName.endsWith(".enc")
            && tlsConfig.secretsUrl.isEmpty
          ) {
            failure(
              "You need to provide a secrets server URL if the server's private key is an encrypted file."
            )
          } else {
            success
          }
        }
      ),
      opt[String]("crt")
        .optional()
        .text(
          "TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set."
        )
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(certChainFile = Some(new File(path))))
        ),
      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the trusted root CA.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(trustCollectionFile = Some(new File(path))))
        ),
      opt[Boolean]("cert-revocation-checking")
        .optional()
        .text(
          "TLS: enable/disable certificate revocation checks with the OCSP. Disabled by default."
        )
        .action((checksEnabled, config) =>
          config.withTlsConfig(c => c.copy(enableCertRevocationChecking = checksEnabled))
        ),
      opt[ClientAuth]("client-auth")
        .optional()
        .text(
          "TLS: The client authentication mode. Must be one of none, optional or require. If TLS is enabled it defaults to require."
        )
        .action((clientAuth, config) => config.withTlsConfig(c => c.copy(clientAuth = clientAuth))),
      opt[TlsVersion]("min-tls-version")
        .optional()
        .text(
          "TLS: Indicates the minimum TLS version to enable. If specified must be either '1.2' or '1.3'."
        )
        .action((tlsVersion, config) =>
          config.withTlsConfig(c => c.copy(minimumServerProtocolVersion = Some(tlsVersion)))
        ),
      opt[Int]("max-commands-in-flight")
        .optional()
        .action((value, config) =>
          config.copy(commandConfig = config.commandConfig.copy(maxCommandsInFlight = value))
        )
        .text(
          s"Maximum number of submitted commands for which the CommandService is waiting to be completed in parallel, for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause new submissions to wait in the queue before being submitted. Default is ${CommandConfiguration.Default.maxCommandsInFlight}."
        ),
      opt[Int]("input-buffer-size")
        .optional()
        .action((value, config) =>
          config.copy(commandConfig = config.commandConfig.copy(inputBufferSize = value))
        )
        .text(
          s"Maximum number of commands waiting to be submitted for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause the server to signal backpressure using the ``RESOURCE_EXHAUSTED`` gRPC status code. Default is ${CommandConfiguration.Default.inputBufferSize}."
        ),
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
        ),
      opt[Duration]("max-deduplication-duration")
        .optional()
        .hidden()
        .action((maxDeduplicationDuration, config) =>
          config
            .copy(maxDeduplicationDuration = Some(maxDeduplicationDuration))
        )
        .text(
          "Maximum command deduplication duration."
        ),
      opt[Int]("max-inbound-message-size")
        .optional()
        .text(
          s"Max inbound message size in bytes. Defaults to ${CliConfig.DefaultMaxInboundMessageSize}."
        )
        .action((maxInboundMessageSize, config) =>
          config.copy(maxInboundMessageSize = maxInboundMessageSize)
        ),
      opt[Int]("events-page-size")
        .optional()
        .text(
          s"Number of events fetched from the index for every round trip when serving streaming calls. Default is ${IndexServiceConfig.DefaultEventsPageSize}."
        )
        .validate { pageSize =>
          if (pageSize > 0) Right(())
          else Left("events-page-size should be strictly positive")
        }
        .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize)),
      opt[Int]("buffered-streams-page-size")
        .optional()
        .text(
          s"Number of transactions fetched from the buffer when serving streaming calls. Default is ${IndexServiceConfig.DefaultBufferedStreamsPageSize}."
        )
        .validate { pageSize =>
          Either.cond(pageSize > 0, (), "buffered-streams-page-size should be strictly positive")
        }
        .action((pageSize, config) => config.copy(bufferedStreamsPageSize = pageSize)),
      opt[Int]("ledger-api-transactions-buffer-max-size")
        .optional()
        .hidden()
        .text(
          s"Maximum size of the in-memory fan-out buffer used for serving Ledger API transaction streams. Default is ${IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize}."
        )
        .validate(bufferSize =>
          Either.cond(
            bufferSize >= 0,
            (),
            "ledger-api-transactions-buffer-max-size must be greater than or equal to 0.",
          )
        )
        .action((maxBufferSize, config) =>
          config.copy(maxTransactionsInMemoryFanOutBufferSize = maxBufferSize)
        ),
      opt[Int]("buffers-prefetching-parallelism")
        .optional()
        .text(
          s"Number of events fetched/decoded in parallel for populating the Ledger API internal buffers. Default is ${IndexServiceConfig.DefaultEventsProcessingParallelism}."
        )
        .validate { buffersPrefetchingParallelism =>
          if (buffersPrefetchingParallelism > 0) Right(())
          else Left("buffers-prefetching-parallelism should be strictly positive")
        }
        .action((eventsProcessingParallelism, config) =>
          config.copy(eventsProcessingParallelism = eventsProcessingParallelism)
        ),
      opt[Int]("acs-id-page-size")
        .optional()
        .text(
          s"Number of contract ids fetched from the index for every round trip when serving ACS calls. Default is ${IndexServiceConfig.DefaultAcsIdPageSize}."
        )
        .validate { acsIdPageSize =>
          if (acsIdPageSize > 0) Right(())
          else Left("acs-id-page-size should be strictly positive")
        }
        .action((acsIdPageSize, config) => config.copy(acsIdPageSize = acsIdPageSize)),
      opt[Int]("acs-id-fetching-parallelism")
        .optional()
        .text(
          s"Number of contract id pages fetched in parallel when serving ACS calls. Default is ${IndexServiceConfig.DefaultAcsIdFetchingParallelism}."
        )
        .validate { acsIdFetchingParallelism =>
          if (acsIdFetchingParallelism > 0) Right(())
          else Left("acs-id-fetching-parallelism should be strictly positive")
        }
        .action((acsIdFetchingParallelism, config) =>
          config.copy(acsIdFetchingParallelism = acsIdFetchingParallelism)
        ),
      opt[Int]("acs-contract-fetching-parallelism")
        .optional()
        .text(
          s"Number of event pages fetched in parallel when serving ACS calls. Default is ${IndexServiceConfig.DefaultAcsContractFetchingParallelism}."
        )
        .validate { acsContractFetchingParallelism =>
          if (acsContractFetchingParallelism > 0) Right(())
          else Left("acs-contract-fetching-parallelism should be strictly positive")
        }
        .action((acsContractFetchingParallelism, config) =>
          config.copy(acsContractFetchingParallelism = acsContractFetchingParallelism)
        ),
      opt[Int]("acs-global-parallelism-limit")
        .optional()
        .text(
          s"Maximum number of concurrent ACS queries to the index database. Default is ${IndexServiceConfig.DefaultAcsGlobalParallelism}."
        )
        .action((acsGlobalParallelism, config) =>
          config.copy(acsGlobalParallelism = acsGlobalParallelism)
        ),
      opt[Long]("max-lf-value-translation-cache-entries")
        .optional()
        .text(
          s"Deprecated parameter --  lf value translation cache doesn't exist anymore."
        )
        .action((_, config) => config),
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
        .hidden(),
      opt[MetricsReporter]("metrics-reporter")
        .optional()
        .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")
        .action((reporter, config) => config.copy(metricsReporter = Some(reporter))),
      opt[Duration]("metrics-reporting-interval")
        .optional()
        .text("Set metric reporting interval.")
        .action((interval, config) => config.copy(metricsReportingInterval = interval)),
      checkConfig(c => {
        val participantIds = c.participants
          .map(pc => pc.participantId)
          .toList

        def filterDuplicates[A](items: List[A]): Set[A] =
          items.groupBy(identity).filter(_._2.size > 1).keySet

        val participantIdDuplicates = filterDuplicates(participantIds)

        if (participantIdDuplicates.nonEmpty)
          failure(
            participantIdDuplicates.mkString(
              "The following participant IDs are duplicate: ",
              ",",
              "",
            )
          )
        else
          success
      }),
      opt[Unit]("early-access")
        .optional()
        .action((_, c) =>
          c.copy(engineConfig =
            c.engineConfig.copy(allowedLanguageVersions = LanguageVersion.EarlyAccessVersions)
          )
        )
        .text(
          "Enable preview version of the next Daml-LF language. Should not be used in production."
        ),
      opt[Unit]("daml-lf-dev-mode-unsafe")
        .optional()
        .hidden()
        .action((_, c) =>
          c.copy(engineConfig =
            c.engineConfig.copy(allowedLanguageVersions = LanguageVersion.DevVersions)
          )
        )
        .text(
          "Enable the development version of the Daml-LF language. Highly unstable. Should not be used in production."
        ),

      // TODO append-only: remove
      opt[Unit]("index-append-only-schema")
        .optional()
        .text("Legacy flag with no effect")
        .action((_, config) => config),

      // TODO remove
      opt[Unit]("mutable-contract-state-cache")
        .optional()
        .hidden()
        .text("Legacy flag with no effect")
        .action((_, config) => config),
      // TODO remove
      opt[Unit]("buffered-ledger-api-streams")
        .optional()
        .hidden()
        .text("Legacy flag with no effect.")
        .action((_, config) => config),
      opt[Boolean]("enable-user-management")
        .optional()
        .text(
          "Whether to enable participant user management."
        )
        .action((enabled, config: CliConfig[Extra]) =>
          config.withUserManagementConfig(_.copy(enabled = enabled))
        ),
      opt[Int]("user-management-cache-expiry")
        .optional()
        .text(
          s"Defaults to ${UserManagementConfig.DefaultCacheExpiryAfterWriteInSeconds} seconds. " +
            "Used to set expiry time for user management cache. " +
            "Also determines the maximum delay for propagating user management state changes which is double its value."
        )
        .action((value, config: CliConfig[Extra]) =>
          config.withUserManagementConfig(_.copy(cacheExpiryAfterWriteInSeconds = value))
        ),
      opt[Int]("user-management-max-cache-size")
        .optional()
        .text(
          s"Defaults to ${UserManagementConfig.DefaultMaxCacheSize} entries. " +
            "Determines the maximum in-memory cache size for user management state."
        )
        .action((value, config: CliConfig[Extra]) =>
          config.withUserManagementConfig(_.copy(maxCacheSize = value))
        ),
      opt[Int]("user-management-max-users-page-size")
        .optional()
        .text(
          s"Maximum number of users that the server can return in a single response. " +
            s"Defaults to ${UserManagementConfig.DefaultMaxUsersPageSize} entries."
        )
        .action((value, config: CliConfig[Extra]) =>
          config.withUserManagementConfig(_.copy(maxUsersPageSize = value))
        ),
      checkConfig(c => {
        val v = c.userManagementConfig.maxUsersPageSize
        if (v == 0 || v >= 100) {
          success
        } else {
          failure(
            s"user-management-max-users-page-size must be either 0 or greater than 99, was: $v"
          )
        }
      }),
      opt[Unit]('s', "static-time")
        .optional()
        .hidden() // Only available for testing purposes
        .action((_, c) => c.copy(timeProviderType = TimeProviderType.Static))
        .text("Use static time. When not specified, wall-clock-time is used."),
      opt[File]("profile-dir")
        .optional()
        .action((dir, c) =>
          c.copy(engineConfig = c.engineConfig.copy(profileDir = Some(dir.toPath)))
        )
        .text("Enable profiling and write the profiles into the given directory."),
      opt[Boolean]("stack-traces")
        .hidden()
        .optional()
        .action((enabled, config) =>
          config.copy(engineConfig = config.engineConfig.copy(stackTraceMode = enabled))
        )
        .text(
          "Enable/disable stack traces. Default is to disable them. " +
            "Enabling stack traces may have a significant performance impact."
        ),
      AuthServiceConfigCli.parse(builder)((v, c) => c.copy(authService = v)),
    )
  }
}
