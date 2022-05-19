// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.runner.common.Config.{
  DefaultEngineConfig,
  DefaultLedgerId,
  DefaultParticipants,
  DefaultParticipantsDatasourceConfig,
}
import com.daml.ledger.runner.common.MetricsConfig.DefaultMetricsConfig
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.{InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import scala.concurrent.duration._
import java.time.Duration

final case class Config(
    engine: EngineConfig = DefaultEngineConfig,
    ledgerId: String = DefaultLedgerId,
    metrics: MetricsConfig = DefaultMetricsConfig,
    dataSource: Map[Ref.ParticipantId, ParticipantDataSourceConfig] =
      DefaultParticipantsDatasourceConfig,
    participants: Map[Ref.ParticipantId, ParticipantConfig] = DefaultParticipants,
)

object Config {
  val DefaultLedgerId: String = "default-ledger-id"
  val DefaultEngineConfig: EngineConfig = EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions,
    profileDir = None,
    stackTraceMode = false,
    forbidV0ContractId = true,
  )
  val DefaultParticipants: Map[Ref.ParticipantId, ParticipantConfig] = Map(
    ParticipantConfig.DefaultParticipantId -> ParticipantConfig()
  )
  val DefaultParticipantsDatasourceConfig: Map[ParticipantId, ParticipantDataSourceConfig] = Map(
    ParticipantConfig.DefaultParticipantId -> ParticipantDataSourceConfig("default-jdbc-url")
  )
  val Default: Config = Config()

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
  )
}
