// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.runner.common.Config
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.engine.{EngineConfig => _EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.{AuthServiceConfig, ApiServerConfig => _ApiServerConfig}
import com.daml.platform.config.MetricsConfig.MetricRegistryType
import com.daml.platform.config.{MetricsConfig, ParticipantConfig => _ParticipantConfig}
import com.daml.platform.configuration.InitialLedgerConfiguration
import com.daml.platform.indexer.{IndexerConfig => _IndexerConfig}
import com.daml.platform.localstore.UserManagementConfig
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.ports.Port
import java.time.Duration
import java.util.UUID

import scala.concurrent.duration._

object SandboxOnXForTest {
  val EngineConfig: _EngineConfig = _EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions,
    profileDir = None,
    stackTraceMode = true,
    forbidV0ContractId = true,
  )
  val DevEngineConfig: _EngineConfig = EngineConfig.copy(
    allowedLanguageVersions = LanguageVersion.DevVersions
  )

  val ApiServerConfig = _ApiServerConfig().copy(
    initialLedgerConfiguration = Some(
      InitialLedgerConfiguration(
        maxDeduplicationDuration = Duration.ofMinutes(30L),
        avgTransactionLatency =
          Configuration.reasonableInitialConfiguration.timeModel.avgTransactionLatency,
        minSkew = Configuration.reasonableInitialConfiguration.timeModel.minSkew,
        maxSkew = Configuration.reasonableInitialConfiguration.timeModel.maxSkew,
        delayBeforeSubmitting = Duration.ZERO,
      )
    ),
    userManagement = UserManagementConfig.default(true),
    maxInboundMessageSize = 4194304,
    configurationLoadTimeout = 10000.millis,
    seeding = Seeding.Strong,
    port = Port.Dynamic,
    managementServiceTimeout = 120000.millis,
  )

  val IndexerConfig = _IndexerConfig(
    inputMappingParallelism = 512
  )

  val ParticipantId: Ref.ParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")
  val ParticipantConfig: _ParticipantConfig =
    _ParticipantConfig(
      apiServer = ApiServerConfig,
      indexer = IndexerConfig,
    )

  def singleParticipant(
      apiServerConfig: _ApiServerConfig = ApiServerConfig,
      indexerConfig: _IndexerConfig = IndexerConfig,
      authentication: AuthServiceConfig = AuthServiceConfig.Wildcard,
  ) = Map(
    ParticipantId -> ParticipantConfig.copy(
      apiServer = apiServerConfig,
      indexer = indexerConfig,
      authentication = authentication,
    )
  )

  def dataSource(jdbcUrl: String): Map[ParticipantId, ParticipantDataSourceConfig] = Map(
    ParticipantId -> ParticipantDataSourceConfig(jdbcUrl)
  )

  val Default: Config = Config(
    engine = EngineConfig,
    dataSource = Config.Default.dataSource.map { case _ -> value => (ParticipantId, value) },
    participants = singleParticipant(),
    metrics = MetricsConfig.DefaultMetricsConfig.copy(registryType = MetricRegistryType.New),
  )

  class ConfigAdaptor(authServiceOverwrite: Option[AuthService]) extends BridgeConfigAdaptor {
    override def authService(participantConfig: _ParticipantConfig): AuthService = {
      authServiceOverwrite.getOrElse(AuthServiceWildcard)
    }
  }
  object ConfigAdaptor {
    def apply(authServiceOverwrite: Option[AuthService]) = new ConfigAdaptor(authServiceOverwrite)
  }

  def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

}
