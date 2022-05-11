// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.platform.indexer.IndexerConfig._
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class IndexerConfig(
    batchingParallelism: Int = DefaultBatchingParallelism,
    database: DbConfig = DefaultDatabase,
    enableCompression: Boolean = DefaultEnableCompression,
    highAvailability: HaConfig = DefaultHaConfig,
    ingestionParallelism: Int = DefaultIngestionParallelism,
    inputMappingParallelism: Int = DefaultInputMappingParallelism,
    maxInputBufferSize: Int = DefaultMaxInputBufferSize,
    restartDelay: FiniteDuration = DefaultRestartDelay,
    startupMode: IndexerStartupMode = DefaultIndexerStartupMode,
    submissionBatchSize: Long = DefaultSubmissionBatchSize,
)

object IndexerConfig {

  def createDefaultDatabaseConfig(jdbcUrl: String): DbConfig = DbConfig(
    jdbcUrl = jdbcUrl,
    // PostgresSQL specific configurations
    // Setting aggressive keep-alive defaults to aid prompt release of the locks on the server side.
    // For reference https://www.postgresql.org/docs/13/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS
    postgres = PostgresDataSourceConfig(
      synchronousCommit = Some(PostgresDataSourceConfig.SynchronousCommitValue.Off),
      tcpKeepalivesIdle = Some(10),
      tcpKeepalivesInterval = Some(1),
      tcpKeepalivesCount = Some(5),
    ),
    connectionPool = ConnectionPoolConfig(
      minimumIdle = DefaultIngestionParallelism + 1, // + 1 for the tailing ledger_end updates
      maxPoolSize = DefaultIngestionParallelism + 1, // + 1 for the tailing ledger_end updates
      // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
      connectionTimeout = FiniteDuration(
        250,
        "millis",
      ),
    ),
  )

  val DefaultIndexerStartupMode: IndexerStartupMode.MigrateAndStart =
    IndexerStartupMode.MigrateAndStart(allowExistingSchema = false)
  val DefaultHaConfig: HaConfig = HaConfig()
  val DefaultUpdatePreparationParallelism = 2
  val DefaultRestartDelay: FiniteDuration = 10.seconds
  val DefaultMaxInputBufferSize: Int = 50
  val DefaultInputMappingParallelism: Int = 16
  val DefaultBatchingParallelism: Int = 4
  val DefaultIngestionParallelism: Int = 16
  val DefaultSubmissionBatchSize: Long = 50L
  val DefaultEnableCompression: Boolean = false
  val DefaultDatabase: DbConfig = DbConfig(
    jdbcUrl = "default-jdbc-url",
    connectionPool = ConnectionPoolConfig(
      minimumIdle = 16,
      maxPoolSize = 16,
      connectionTimeout = 250.millis,
    ),
  )
}
