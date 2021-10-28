// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.lf.data.Ref
import com.daml.platform.configuration.IndexConfiguration
import com.daml.platform.indexer.IndexerConfig._
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.store.DbType

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class IndexerConfig(
    participantId: Ref.ParticipantId,
    jdbcUrl: String,
    startupMode: IndexerStartupMode,
    databaseConnectionTimeout: FiniteDuration = DefaultDatabaseConnectionTimeout,
    restartDelay: FiniteDuration = DefaultRestartDelay,
    eventsPageSize: Int = IndexConfiguration.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = IndexConfiguration.DefaultEventsProcessingParallelism,
    updatePreparationParallelism: Int = DefaultUpdatePreparationParallelism,
    allowExistingSchema: Boolean = false,
    asyncCommitMode: DbType.AsyncCommitMode = DefaultAsyncCommitMode,
    maxInputBufferSize: Int = DefaultMaxInputBufferSize,
    inputMappingParallelism: Int = DefaultInputMappingParallelism,
    batchingParallelism: Int = DefaultBatchingParallelism,
    ingestionParallelism: Int = DefaultIngestionParallelism,
    submissionBatchSize: Long = DefaultSubmissionBatchSize,
    tailingRateLimitPerSecond: Int = DefaultTailingRateLimitPerSecond,
    batchWithinMillis: Long = DefaultBatchWithinMillis,
    enableCompression: Boolean = DefaultEnableCompression,
    schemaMigrationAttempts: Int = DefaultSchemaMigrationAttempts,
    schemaMigrationAttemptBackoff: FiniteDuration = DefaultSchemaMigrationAttemptBackoff,
    haConfig: HaConfig = HaConfig(),
    // PostgresSQL specific configurations
    // Setting aggressive keep-alive defaults to aid prompt release of the locks on the server side.
    // For reference https://www.postgresql.org/docs/13/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS
    postgresTcpKeepalivesIdle: Option[Int] = Some(10),
    postgresTcpKeepalivesInterval: Option[Int] = Some(1),
    postgresTcpKeepalivesCount: Option[Int] = Some(5),
)

object IndexerConfig {

  val DefaultUpdatePreparationParallelism = 2
  val DefaultRestartDelay: FiniteDuration = 10.seconds
  val DefaultDatabaseConnectionTimeout: FiniteDuration = 250.millis
  val DefaultAsyncCommitMode: DbType.AsyncCommitMode = DbType.AsynchronousCommit

  val DefaultMaxInputBufferSize: Int = 50
  val DefaultInputMappingParallelism: Int = 16
  val DefaultBatchingParallelism: Int = 4
  val DefaultIngestionParallelism: Int = 16
  val DefaultSubmissionBatchSize: Long = 50L
  val DefaultTailingRateLimitPerSecond: Int = 20
  val DefaultBatchWithinMillis: Long = 50L
  val DefaultEnableCompression: Boolean = false

  val DefaultSchemaMigrationAttempts: Int = 30
  val DefaultSchemaMigrationAttemptBackoff: FiniteDuration = 1.second
}
