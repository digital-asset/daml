// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.runner.common.MetricsConfig.MetricRegistryType
import com.daml.ledger.runner.common.{Config, MetricsConfig, ParticipantConfig}
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.{InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration._

object SandboxOnXForTest {
  val SandboxEngineConfig = EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions,
    profileDir = None,
    stackTraceMode = true,
    forbidV0ContractId = true,
  )
  val SandboxParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")
  val SandboxParticipantConfig =
    ParticipantConfig(
      apiServer = ApiServerConfig().copy(
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
        party = PartyConfiguration(implicitPartyAllocation = true),
        seeding = Seeding.Strong,
        port = Port(0),
        managementServiceTimeout = 120000.millis,
      ),
      indexer = IndexerConfig(
        inputMappingParallelism = 512
      ),
    )
  val SandboxDefault = Config(
    engine = SandboxEngineConfig,
    dataSource = Config.Default.dataSource.map { case _ -> value => (SandboxParticipantId, value) },
    participants = Map(SandboxParticipantId -> SandboxParticipantConfig),
    metrics = MetricsConfig.DefaultMetricsConfig.copy(registryType = MetricRegistryType.New),
  )

  class SandboxOnXForTestConfigAdaptor(authServiceOverwrite: Option[AuthService])
      extends BridgeConfigAdaptor {
    override def authService(apiServerConfig: ApiServerConfig): AuthService = {
      authServiceOverwrite.getOrElse(AuthServiceWildcard)
    }
  }

  def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

}
