// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.configuration.Configuration
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{
  IndexConfiguration,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.LfValueTranslationCache
import io.grpc.ServerInterceptor
import scopt.OptionParser

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

trait ConfigProvider[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[LegacyCliConfig[ExtraConfig]]): Unit

  private def toParticipantConfig(
      cliConfig: LegacyCliConfig[ExtraConfig],
      config: LegacyCliParticipantConfig,
  ): ParticipantConfig = ParticipantConfig(
    runMode = config.mode,
    participantId = config.participantId,
    shardName = config.shardName,
    indexer = config.indexerConfig,
    index = IndexConfiguration(
      acsContractFetchingParallelism = cliConfig.acsContractFetchingParallelism,
      acsGlobalParallelism = cliConfig.acsGlobalParallelism,
      acsIdFetchingParallelism = cliConfig.acsIdFetchingParallelism,
      acsIdPageSize = cliConfig.acsIdPageSize,
      enableInMemoryFanOutForLedgerApi = cliConfig.enableInMemoryFanOutForLedgerApi,
      eventsPageSize = cliConfig.eventsPageSize,
      bufferedStreamsPageSize = cliConfig.bufferedStreamsPageSize,
      eventsProcessingParallelism = cliConfig.eventsProcessingParallelism,
      maxContractStateCacheSize = config.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
      archiveFiles = IndexConfiguration.DefaultArchiveFiles,
    ),
    lfValueTranslationCache = LfValueTranslationCache.Config(
      contractsMaximumSize = cliConfig.lfValueTranslationContractCache,
      eventsMaximumSize = cliConfig.lfValueTranslationEventCache,
    ),
    maxDeduplicationDuration = cliConfig.maxDeduplicationDuration,
    apiServer = ApiServerConfig(
      port = config.port,
      address = config.address,
      tls = cliConfig.tlsConfig,
      maxInboundMessageSize = cliConfig.maxInboundMessageSize,
      initialLedgerConfiguration = Some(initialLedgerConfig(cliConfig.maxDeduplicationDuration)),
      configurationLoadTimeout = FiniteDuration(
        cliConfig.configurationLoadTimeout.toMillis,
        TimeUnit.MILLISECONDS,
      ),
      portFile = config.portFile,
      seeding = cliConfig.seeding,
      managementServiceTimeout = FiniteDuration(
        config.managementServiceTimeout.toMillis,
        TimeUnit.MILLISECONDS,
      ),
      userManagement = cliConfig.userManagementConfig,
      authentication = cliConfig.authService,
      command = cliConfig.commandConfig,
      party = partyConfig(cliConfig.extra),
      timeProviderType = cliConfig.timeProviderType,
      database = DbConfig(
        jdbcUrl = config.serverJdbcUrl,
        connectionPool = ConnectionPoolConfig(
          config.apiServerDatabaseConnectionPoolSize,
          config.apiServerDatabaseConnectionPoolSize,
          connectionTimeout = FiniteDuration(
            config.apiServerDatabaseConnectionTimeout.toMillis,
            TimeUnit.MILLISECONDS,
          ),
        ),
      ),
    ),
  )

  def fromLegacyCliConfig(config: LegacyCliConfig[ExtraConfig]): Config = {
    Config(
      engine = config.engineConfig,
      ledgerId = config.ledgerId,
      metrics = MetricsConfig(
        reporter = config.metricsReporter,
        reportingInterval = config.metricsReportingInterval.toScala,
      ),
      participants = config.participants.map { participantConfig =>
        ParticipantName.fromParticipantId(
          participantConfig.participantId,
          participantConfig.shardName,
        ) -> toParticipantConfig(config, participantConfig)
      }.toMap,
    )
  }

  def partyConfig(@unused extra: ExtraConfig): PartyConfiguration = PartyConfiguration.default

  def initialLedgerConfig(
      maxDeduplicationDuration: Option[Duration]
  ): InitialLedgerConfiguration = {
    val conf = Configuration.reasonableInitialConfiguration
    InitialLedgerConfiguration(
      maxDeduplicationDuration = maxDeduplicationDuration.getOrElse(
        conf.maxDeduplicationDuration
      ),
      avgTransactionLatency = conf.timeModel.avgTransactionLatency,
      minSkew = conf.timeModel.minSkew,
      maxSkew = conf.timeModel.maxSkew,
      generation = conf.generation,
      // If a new index database is added to an already existing ledger,
      // a zero delay will likely produce a "configuration rejected" ledger entry,
      // because at startup the indexer hasn't ingested any configuration change yet.
      // Override this setting for distributed ledgers where you want to avoid these superfluous entries.
      delayBeforeSubmitting = Duration.ZERO,
    )
  }

  def timeServiceBackend(config: ApiServerConfig): Option[TimeServiceBackend] =
    config.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  def interceptors: List[ServerInterceptor] = List.empty

  def authService(apiServerConfig: ApiServerConfig): AuthService =
    apiServerConfig.authentication.create()
}

object ConfigProvider {
  class ForUnit extends ConfigProvider[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[LegacyCliConfig[Unit]]): Unit = ()
  }

  object ForUnit extends ForUnit
}
