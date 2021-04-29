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
    databaseConnectionPoolSize: Int = ParticipantIndexerConfig.DefaultDatabaseConnectionPoolSize,
    inputMappingParallelism: Int = ParticipantIndexerConfig.DefaultInputMappingParallelism,
    batchingParallelism: Int = ParticipantIndexerConfig.DefaultBatchingParallelism,
    ingestionParallelism: Int = ParticipantIndexerConfig.DefaultIngestionParallelism,
    submissionBatchSize: Long = ParticipantIndexerConfig.DefaultSubmissionBatchSize,
    tailingRateLimitPerSecond: Int = ParticipantIndexerConfig.DefaultTailingRateLimitPerSecond,
    batchWithinMillis: Long = ParticipantIndexerConfig.DefaultBatchWithinMillis,
    enableCompression: Boolean = ParticipantIndexerConfig.DefaultEnableCompression,
)

object ParticipantIndexerConfig {
  val DefaultDatabaseConnectionPoolSize: Int = IndexerConfig.DefaultDatabaseConnectionPoolSize
  val DefaultInputMappingParallelism: Int = IndexerConfig.DefaultInputMappingParallelism
  val DefaultBatchingParallelism: Int = IndexerConfig.DefaultBatchingParallelism
  val DefaultIngestionParallelism: Int = IndexerConfig.DefaultIngestionParallelism
  val DefaultSubmissionBatchSize: Long = IndexerConfig.DefaultSubmissionBatchSize
  val DefaultTailingRateLimitPerSecond: Int = IndexerConfig.DefaultTailingRateLimitPerSecond
  val DefaultBatchWithinMillis: Long = IndexerConfig.DefaultBatchWithinMillis
  val DefaultEnableCompression: Boolean = IndexerConfig.DefaultEnableCompression
}
