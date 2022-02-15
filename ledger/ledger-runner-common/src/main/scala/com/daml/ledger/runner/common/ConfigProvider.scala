// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.configuration.Configuration
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.platform.services.time.TimeProviderType
import io.grpc.ServerInterceptor
import scopt.OptionParser

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration

trait ConfigProvider[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): IndexerConfig =
    IndexerConfig(
      participantConfig.participantId,
      jdbcUrl = participantConfig.serverJdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      eventsPageSize = config.eventsPageSize,
      eventsProcessingParallelism = config.eventsProcessingParallelism,
      allowExistingSchema = participantConfig.indexerConfig.allowExistingSchema,
      maxInputBufferSize = participantConfig.indexerConfig.maxInputBufferSize,
      inputMappingParallelism = participantConfig.indexerConfig.inputMappingParallelism,
      ingestionParallelism = participantConfig.indexerConfig.ingestionParallelism,
      batchingParallelism = participantConfig.indexerConfig.batchingParallelism,
      submissionBatchSize = participantConfig.indexerConfig.submissionBatchSize,
      tailingRateLimitPerSecond = participantConfig.indexerConfig.tailingRateLimitPerSecond,
      batchWithinMillis = participantConfig.indexerConfig.batchWithinMillis,
      enableCompression = participantConfig.indexerConfig.enableCompression,
    )

  def apiServerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): ApiServerConfig =
    ApiServerConfig(
      participantId = participantConfig.participantId,
      archiveFiles = Nil,
      port = participantConfig.port,
      address = participantConfig.address,
      jdbcUrl = participantConfig.serverJdbcUrl,
      databaseConnectionPoolSize = participantConfig.apiServerDatabaseConnectionPoolSize,
      databaseConnectionTimeout = FiniteDuration(
        participantConfig.apiServerDatabaseConnectionTimeout.toMillis,
        TimeUnit.MILLISECONDS,
      ),
      tlsConfig = config.tlsConfig,
      maxInboundMessageSize = config.maxInboundMessageSize,
      initialLedgerConfiguration = Some(initialLedgerConfig(config)),
      configurationLoadTimeout = config.configurationLoadTimeout,
      eventsPageSize = config.eventsPageSize,
      eventsProcessingParallelism = config.eventsProcessingParallelism,
      acsIdPageSize = config.acsIdPageSize,
      acsIdFetchingParallelism = config.acsIdFetchingParallelism,
      acsContractFetchingParallelism = config.acsContractFetchingParallelism,
      acsGlobalParallelism = config.acsGlobalParallelism,
      acsIdQueueLimit = config.acsIdQueueLimit,
      portFile = participantConfig.portFile,
      seeding = config.seeding,
      managementServiceTimeout = participantConfig.managementServiceTimeout,
      maxContractStateCacheSize = participantConfig.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = participantConfig.maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize =
        participantConfig.maxTransactionsInMemoryFanOutBufferSize,
      enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
      userManagementConfig = config.userManagementConfig,
    )

  def partyConfig(@unused config: Config[ExtraConfig]): PartyConfiguration =
    PartyConfiguration.default

  def initialLedgerConfig(config: Config[ExtraConfig]): InitialLedgerConfiguration = {
    InitialLedgerConfiguration(
      configuration = Configuration.reasonableInitialConfiguration.copy(maxDeduplicationTime =
        config.maxDeduplicationDuration.getOrElse(
          Configuration.reasonableInitialConfiguration.maxDeduplicationTime
        )
      ),
      // If a new index database is added to an already existing ledger,
      // a zero delay will likely produce a "configuration rejected" ledger entry,
      // because at startup the indexer hasn't ingested any configuration change yet.
      // Override this setting for distributed ledgers where you want to avoid these superfluous entries.
      delayBeforeSubmitting = Duration.ZERO,
    )
  }

  def timeServiceBackend(config: Config[ExtraConfig]): Option[TimeServiceBackend] =
    config.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  def authService(@unused config: Config[ExtraConfig]): AuthService =
    config.authService

  def interceptors(@unused config: Config[ExtraConfig]): List[ServerInterceptor] =
    List.empty

  def createMetrics(
      participantConfig: ParticipantConfig,
      @unused config: Config[ExtraConfig],
  ): Metrics = {
    val registryName = participantConfig.participantId + participantConfig.shardName
      .map("-" + _)
      .getOrElse("")
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
  }
}

object ConfigProvider {
  class ForUnit extends ConfigProvider[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()
  }

  object ForUnit extends ForUnit
}
