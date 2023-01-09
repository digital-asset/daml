// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.config.{MetricsConfig, ParticipantConfig}
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DataSourceProperties,
  ParticipantDataSourceConfig,
}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

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
      timeProviderType = cliConfig.timeProviderType,
    ),
  )

  def toConfig(configAdaptor: ConfigAdaptor, config: CliConfig[_]): Config = {
    Config(
      engine = config.engineConfig,
      ledgerId = config.ledgerId,
      metrics = MetricsConfig(
        reporter = config.metricsReporter.getOrElse(MetricsConfig.DefaultMetricsConfig.reporter)
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
