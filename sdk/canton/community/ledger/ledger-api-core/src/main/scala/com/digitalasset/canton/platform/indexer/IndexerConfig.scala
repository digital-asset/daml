// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.platform.indexer.IndexerConfig.*
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DataSourceProperties}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** See com.digitalasset.canton.platform.indexer.JdbcIndexer for semantics on these configurations.
  */
final case class IndexerConfig(
    batchingParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultBatchingParallelism),
    enableCompression: Boolean = DefaultEnableCompression,
    ingestionParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultIngestionParallelism),
    inputMappingParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DefaultInputMappingParallelism),
    maxInputBufferSize: NonNegativeInt = NonNegativeInt.tryCreate(DefaultMaxInputBufferSize),
    restartDelay: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofSeconds(DefaultRestartDelay.toSeconds),
    submissionBatchSize: Long = DefaultSubmissionBatchSize,
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
)

object IndexerConfig {

  // Exposed as public method so defaults can be overriden in the downstream code.
  def createDataSourcePropertiesForTesting(
      ingestionParallelism: Int
  ): DataSourceProperties = DataSourceProperties(
    // PostgresSQL specific configurations
    postgres = PostgresDataSourceConfig(
      synchronousCommit = Some(PostgresDataSourceConfig.SynchronousCommitValue.Off)
    ),
    connectionPool = createConnectionPoolConfig(ingestionParallelism),
  )

  def createConnectionPoolConfig(
      ingestionParallelism: Int,
      connectionTimeout: FiniteDuration = FiniteDuration(
        // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
        250,
        "millis",
      ),
  ): ConnectionPoolConfig =
    ConnectionPoolConfig(
      connectionPoolSize =
        ingestionParallelism + 2, // + 2 for the tailing ledger_end and post processing end updates
      connectionTimeout = connectionTimeout,
    )

  val DefaultRestartDelay: FiniteDuration = 10.seconds
  val DefaultMaxInputBufferSize: Int = 50
  val DefaultInputMappingParallelism: Int = 16
  val DefaultBatchingParallelism: Int = 4
  val DefaultIngestionParallelism: Int = 16
  val DefaultSubmissionBatchSize: Long = 50L
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
}
