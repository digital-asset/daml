// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.platform.configuration.IndexConfiguration
import com.daml.platform.indexer.IndexerConfig._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class IndexerConfig(
    participantId: ParticipantId,
    jdbcUrl: String,
    startupMode: IndexerStartupMode,
    databaseConnectionPoolSize: Int = DefaultDatabaseConnectionPoolSize,
    restartDelay: FiniteDuration = DefaultRestartDelay,
    eventsPageSize: Int = IndexConfiguration.DefaultEventsPageSize,
    updatePreparationParallelism: Int = DefaultUpdatePreparationParallelism,
    allowExistingSchema: Boolean = false,
    inputMappingParallelism: Int = 3,
    ingestionParallelism: Int = 4,
    submissionBatchSize: Long = 1L,
    tailingRateLimitPerSecond: Int = 100,
    batchWithinMillis: Long = 10,
)

object IndexerConfig {

  val DefaultUpdatePreparationParallelism = 2
  val DefaultRestartDelay: FiniteDuration = 10.seconds
  // Should be greater than or equal to the number of pipline stages
  val DefaultDatabaseConnectionPoolSize: Int = 3

}
