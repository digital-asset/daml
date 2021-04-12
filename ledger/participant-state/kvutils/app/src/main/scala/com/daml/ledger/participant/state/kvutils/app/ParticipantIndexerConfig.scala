// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.platform.indexer.IndexerConfig

/** Indexer-specific configuration of a participant.
  *
  * Parameters that are shared between the indexer and the ledger API server are stored in the parent [[ParticipantConfig]].
  * Parameters that are shared between all ledger participants are stored in the parent [[Config]]
  */
final case class ParticipantIndexerConfig(
    allowExistingSchema: Boolean,
    databaseConnectionPoolSize: Int = ParticipantIndexerConfig.defaultDatabaseConnectionPoolSize,
    inputMappingParallelism: Int = ParticipantIndexerConfig.defaultInputMappingParallelism,
    ingestionParallelism: Int = ParticipantIndexerConfig.defaultIngestionParallelism,
    submissionBatchSize: Long = ParticipantIndexerConfig.defaultSubmissionBatchSize,
    tailingRateLimitPerSecond: Int = ParticipantIndexerConfig.defaultTailingRateLimitPerSecond,
    batchWithinMillis: Long = ParticipantIndexerConfig.defaultBatchWithinMillis,
    enableCompression: Boolean = ParticipantIndexerConfig.defaultEnableCompression,
)

object ParticipantIndexerConfig {
  val defaultDatabaseConnectionPoolSize: Int = IndexerConfig.DefaultDatabaseConnectionPoolSize
  val defaultInputMappingParallelism: Int = IndexerConfig.DefaultInputMappingParallelism
  val defaultIngestionParallelism: Int = IndexerConfig.DefaultIngestionParallelism
  val defaultSubmissionBatchSize: Long = IndexerConfig.DefaultSubmissionBatchSize
  val defaultTailingRateLimitPerSecond: Int = IndexerConfig.DefaultTailingRateLimitPerSecond
  val defaultBatchWithinMillis: Long = IndexerConfig.DefaultBatchWithinMillis
  val defaultEnableCompression: Boolean = IndexerConfig.DefaultEnableCompression
}
