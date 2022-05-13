// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.runner.common.Config.{
  DefaultEngineConfig,
  DefaultLedgerId,
  DefaultParticipants,
}
import com.daml.ledger.runner.common.MetricsConfig.DefaultMetricsConfig
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion

final case class Config(
    engine: EngineConfig = DefaultEngineConfig,
    ledgerId: String = DefaultLedgerId,
    metrics: MetricsConfig = DefaultMetricsConfig,
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
  val Default: Config = Config()
}
