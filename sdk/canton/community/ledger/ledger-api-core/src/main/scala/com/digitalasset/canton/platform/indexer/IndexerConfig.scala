// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.platform.indexer.IndexerConfig.*
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DataSourceProperties}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** See com.digitalasset.canton.platform.indexer.JdbcIndexer for semantics on these configurations.
  *
  *   - enableCompression: switches on compression for both consuming and non-consuming exercises,
  *     equivalent to setting both enableCompressionConsumingExercise and
  *     enableCompressionNonConsumingExercise to true. This is to maintain backward compatibility
  *     with existing config files.
  *   - enableCompressionConsumingExercise: switches on compression for consuming exercises
  *   - enableCompressionNonConsumingExercise: switches on compression for non-consuming exercises
  */
final case class IndexerConfig(
    batchingParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultBatchingParallelism),
    enableCompression: Boolean = DefaultEnableCompression,
    enableCompressionConsumingExercise: Boolean = DefaultEnableCompression,
    enableCompressionNonConsumingExercise: Boolean = DefaultEnableCompression,
    ingestionParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultIngestionParallelism),
    inputMappingParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DefaultInputMappingParallelism),
    dbPrepareParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultDbPrepareParallelism),
    maxInputBufferSize: NonNegativeInt = NonNegativeInt.tryCreate(DefaultMaxInputBufferSize),
    restartDelay: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(DefaultRestartDelay.toSeconds),
    useWeightedBatching: Boolean =
      DefaultUseWeightedBatching, // feature flag to enable improved batching strategy in ingestion pipeline
    submissionBatchSize: Long = DefaultSubmissionBatchSize,
    submissionBatchInsertionSize: Long = DefaultSubmissionBatchInsertionSize,
    maxOutputBatchedBufferSize: Int = DefaultMaxOutputBatchedBufferSize,
    maxTailerBatchSize: Int = DefaultMaxTailerBatchSize,
    postProcessingParallelism: Int = DefaultPostProcessingParallelism,
    queueMaxBlockedOffer: Int = DefaultQueueMaxBlockedOffer,
    queueBufferSize: Int = DefaultQueueBufferSize,
    queueUncommittedWarnThreshold: Int = DefaultQueueUncommittedWarnThreshold,
    queueRecoveryRetryMinWaitMillis: Int = DefaultQueueRecoveryRetryMinWaitMillis,
    queueRecoveryRetryMaxWaitMillis: Int = DefaultQueueRecoveryRetryMaxWaitMillis,
    queueRecoveryRetryAttemptWarnThreshold: Int = DefaultQueueRecoveryRetryAttemptWarnThreshold,
    queueRecoveryRetryAttemptErrorThreshold: Int = DefaultQueueRecoveryRetryAttemptErrorThreshold,
    disableMonotonicityChecks: Boolean = false,
    postgresDataSource: PostgresDataSourceConfig = DefaultPostgresDataSourceConfig,
)

object IndexerConfig {

  // Exposed as public method so defaults can be overridden in the downstream code.
  def createDataSourcePropertiesForTesting(
      indexerConfig: IndexerConfig
  ): DataSourceProperties = DataSourceProperties(
    // PostgresSQL specific configurations
    postgres = PostgresDataSourceConfig(
      synchronousCommit = Some(PostgresDataSourceConfig.SynchronousCommitValue.Off)
    ),
    connectionPool = createConnectionPoolConfig(indexerConfig),
  )

  def createConnectionPoolConfig(
      indexerConfig: IndexerConfig,
      connectionTimeout: FiniteDuration = FiniteDuration(
        // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
        250,
        "millis",
      ),
  ): ConnectionPoolConfig =
    ConnectionPoolConfig(
      connectionPoolSize =
        indexerConfig.ingestionParallelism.unwrap + indexerConfig.dbPrepareParallelism.unwrap +
          2, // + 2 for the tailing ledger_end and post processing end updates
      connectionTimeout = connectionTimeout,
    )

  val DefaultRestartDelay: FiniteDuration = 10.seconds
  val DefaultMaxInputBufferSize: Int = 50
  val DefaultInputMappingParallelism: Int = 16
  val DefaultDbPrepareParallelism: Int = 4
  val DefaultBatchingParallelism: Int = 4
  val DefaultIngestionParallelism: Int = 16
  val DefaultUseWeightedBatching: Boolean = false
  val DefaultSubmissionBatchSize: Long = 50L
  val DefaultSubmissionBatchInsertionSize: Long = 5000L
  val DefaultEnableCompression: Boolean = false
  val DefaultMaxOutputBatchedBufferSize: Int = 16
  val DefaultMaxTailerBatchSize: Int = 10
  val DefaultPostProcessingParallelism: Int = 8
  val DefaultQueueMaxBlockedOffer: Int = 1000
  val DefaultQueueBufferSize: Int = 50
  val DefaultQueueUncommittedWarnThreshold: Int = 5000
  val DefaultQueueRecoveryRetryMinWaitMillis: Int = 50
  val DefaultQueueRecoveryRetryMaxWaitMillis: Int = 5000
  val DefaultQueueRecoveryRetryAttemptWarnThreshold: Int = 50
  val DefaultQueueRecoveryRetryAttemptErrorThreshold: Int = 100
  val DefaultPostgresDataSourceConfig: PostgresDataSourceConfig =
    PostgresDataSourceConfig(networkTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(20)))
}
