// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.config.{MetricsConfig, ParticipantConfig}
import com.daml.platform.configuration.{IndexServiceConfig, PartyConfiguration}
import com.daml.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.daml.platform.store.LfValueTranslationCache

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

object LegacyCliConfigConverter {

  private def toParticipantConfig(
      configAdaptor: ConfigAdaptor,
      cliConfig: CliConfig[_],
      config: CliParticipantConfig,
  ): ParticipantConfig = ParticipantConfig(
    authentication = cliConfig.authService,
    indexer = config.indexerConfig,
    indexService = IndexServiceConfig(
      acsContractFetchingParallelism = cliConfig.acsContractFetchingParallelism,
      acsGlobalParallelism = cliConfig.acsGlobalParallelism,
      acsIdFetchingParallelism = cliConfig.acsIdFetchingParallelism,
      acsIdPageSize = cliConfig.acsIdPageSize,
      eventsPageSize = cliConfig.eventsPageSize,
      bufferedStreamsPageSize = cliConfig.bufferedStreamsPageSize,
      eventsProcessingParallelism = cliConfig.eventsProcessingParallelism,
      maxContractStateCacheSize = config.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize = cliConfig.maxTransactionsInMemoryFanOutBufferSize,
      inMemoryStateUpdaterParallelism = IndexServiceConfig.DefaultInMemoryStateUpdaterParallelism,
    ),
    lfValueTranslationCache = LfValueTranslationCache.Config(
      contractsMaximumSize = cliConfig.lfValueTranslationContractCache.maximumSize,
      eventsMaximumSize = cliConfig.lfValueTranslationEventCache.maximumSize,
    ),
    dataSourceProperties = DataSourceProperties(
      connectionPool = ConnectionPoolConfig(
        connectionPoolSize = config.apiServerDatabaseConnectionPoolSize,
        connectionTimeout = FiniteDuration(
          config.apiServerDatabaseConnectionTimeout.toMillis,
          TimeUnit.MILLISECONDS,
        ),
      )
    ),
    apiServer = ApiServerConfig(
      port = config.port,
      address = config.address,
      tls = cliConfig.tlsConfig,
      maxInboundMessageSize = cliConfig.maxInboundMessageSize,
      initialLedgerConfiguration =
        Some(configAdaptor.initialLedgerConfig(cliConfig.maxDeduplicationDuration)),
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
      command = cliConfig.commandConfig,
      party = PartyConfiguration.Default
        .copy(implicitPartyAllocation = cliConfig.implicitPartyAllocation),
      timeProviderType = cliConfig.timeProviderType,
    ),
  )

  def toConfig(configAdaptor: ConfigAdaptor, config: CliConfig[_]): Config = {
    Config(
      engine = config.engineConfig,
      ledgerId = config.ledgerId,
      metrics = MetricsConfig(
        enabled = config.metricsReporter.isDefined,
        reporter = config.metricsReporter.getOrElse(MetricsConfig.DefaultMetricsConfig.reporter),
        reportingInterval = config.metricsReportingInterval.toScala,
      ),
      dataSource = config.participants.map { participantConfig =>
        participantConfig.participantId -> ParticipantDataSourceConfig(
          participantConfig.serverJdbcUrl
        )
      }.toMap,
      participants = config.participants.map { participantConfig =>
        participantConfig.participantId -> toParticipantConfig(
          configAdaptor,
          config,
          participantConfig,
        )
      }.toMap,
    )
  }

}
