// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.configuration.Configuration
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{IndexConfiguration, InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.RateLimitingConfig
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

  def apiServerConfig(
      participantConfig: ParticipantConfig,
      rateLimitingProvider: Option[RateLimitingConfig],
      config: Config[ExtraConfig],
  ): ApiServerConfig =
    ApiServerConfig(
      participantId = participantConfig.participantId,
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
      indexConfiguration = IndexConfiguration(
        eventsPageSize = config.eventsPageSize,
        eventsProcessingParallelism = config.eventsProcessingParallelism,
        bufferedStreamsPageSize = config.bufferedStreamsPageSize,
        acsIdPageSize = config.acsIdPageSize,
        acsIdFetchingParallelism = config.acsIdFetchingParallelism,
        acsContractFetchingParallelism = config.acsContractFetchingParallelism,
        acsGlobalParallelism = config.acsGlobalParallelism,
        maxContractStateCacheSize = participantConfig.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = participantConfig.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          participantConfig.maxTransactionsInMemoryFanOutBufferSize,
        enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
        archiveFiles = Nil,
      ),
      portFile = participantConfig.portFile,
      seeding = config.seeding,
      managementServiceTimeout = participantConfig.managementServiceTimeout,
      userManagementConfig = config.userManagementConfig,
      rateLimitingConfig = rateLimitingProvider
    )

  def partyConfig(@unused config: Config[ExtraConfig]): PartyConfiguration =
    PartyConfiguration.default

  def initialLedgerConfig(config: Config[ExtraConfig]): InitialLedgerConfiguration = {
    InitialLedgerConfiguration(
      configuration = Configuration.reasonableInitialConfiguration.copy(maxDeduplicationDuration =
        config.maxDeduplicationDuration.getOrElse(
          Configuration.reasonableInitialConfiguration.maxDeduplicationDuration
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

}

object ConfigProvider {
  class ForUnit extends ConfigProvider[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()
  }

  object ForUnit extends ForUnit
}
