// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.caching
import com.daml.ledger.runner.common.ParticipantConfig._
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.store.LfValueTranslationCache

final case class ParticipantConfig(
    apiServer: ApiServerConfig = DefaultApiServer,
    indexService: IndexServiceConfig = DefaultIndexConfig,
    indexer: IndexerConfig = DefaultIndexerConfig,
    lfValueTranslationCache: LfValueTranslationCache.Config = DefaultLfValueTranslationCache,
    runMode: ParticipantRunMode = DefaultRunMode,
)

object ParticipantConfig {
  val DefaultParticipantId: Ref.ParticipantId = Ref.ParticipantId.assertFromString("default")
  val DefaultShardName: Option[String] = None
  val DefaultRunMode: ParticipantRunMode = ParticipantRunMode.Combined
  val DefaultIndexerConfig: IndexerConfig = IndexerConfig()
  val DefaultIndexConfig: IndexServiceConfig = IndexServiceConfig()
  val DefaultLfValueTranslationCache: LfValueTranslationCache.Config =
    LfValueTranslationCache.Config(
      eventsMaximumSize = caching.SizedCache.Configuration.none,
      contractsMaximumSize = caching.SizedCache.Configuration.none,
    )
  val DefaultApiServer: ApiServerConfig = ApiServerConfig()
}
