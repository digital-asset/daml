// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.runner.common.Config.{
  DefaultEngineConfig,
  DefaultLedgerId,
  DefaultParticipants,
  DefaultParticipantsDatasourceConfig,
}
import com.daml.platform.config.MetricsConfig.DefaultMetricsConfig
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.platform.config.{MetricsConfig, ParticipantConfig}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig

final case class Config(
    engine: EngineConfig = DefaultEngineConfig,
    ledgerId: String = DefaultLedgerId,
    metrics: MetricsConfig = DefaultMetricsConfig,
    dataSource: Map[Ref.ParticipantId, ParticipantDataSourceConfig] =
      DefaultParticipantsDatasourceConfig,
    participants: Map[Ref.ParticipantId, ParticipantConfig] = DefaultParticipants,
) {
  def withDataSource(dataSource: Map[Ref.ParticipantId, ParticipantDataSourceConfig]): Config =
    copy(dataSource = dataSource)
}

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
    ParticipantConfig.DefaultParticipantId -> ParticipantDataSourceConfig(
      "jdbc:h2:mem:default;db_close_delay=-1;db_close_on_exit=false"
    )
  )
  val Default: Config = Config()
}
