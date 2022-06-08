// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.stream.KillSwitch
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.{DbSupport, FlywayMigrations}

import scala.concurrent.ExecutionContext

private[platform] case class InitializeParticipant(
    // TODO LLP: Not right to use IndexerConfig
    config: IndexerConfig,
    lapiDbSupport: DbSupport,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    providedParticipantId: Ref.ParticipantId,
    metrics: Metrics,
    participantConfig: ParticipantConfig,
    ec: ExecutionContext,
)(implicit loggingContext: LoggingContext) {

  def owner(): ResourceOwner[ParticipantInMemoryState] = {
    implicit val executionContext: ExecutionContext = ec
    for {
      _ <- migrate(
        new FlywayMigrations(
          participantDataSourceConfig.jdbcUrl,
          Seq.empty, // TODO LLP: Additional migration paths
        )(ResourceContext(executionContext), loggingContext)
      )(participantConfig.indexer.startupMode)
      participantInMemoryState <- ParticipantInMemoryState.owner(
        ledgerEnd = LedgerEnd.beforeBegin,
        apiStreamShutdownTimeout = participantConfig.indexService.apiStreamShutdownTimeout,
        bufferedStreamsPageSize = participantConfig.indexService.bufferedStreamsPageSize,
        maxContractStateCacheSize = participantConfig.indexService.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = participantConfig.indexService.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          participantConfig.indexService.maxTransactionsInMemoryFanOutBufferSize,
        cachesUpdaterExecutionContext = executionContext, // TODO LLP: Dedicated ExecutionContext
        servicesExecutionContext = executionContext,
        metrics = metrics,
      )
    } yield participantInMemoryState
  }

  private def migrate(
      flywayMigrations: FlywayMigrations
  )(startupMode: IndexerStartupMode): ResourceOwner[Unit] =
    ResourceOwner.forFuture(() =>
      startupMode match {
        case IndexerStartupMode.MigrateAndStart(allowExistingSchema) =>
          flywayMigrations.migrate(allowExistingSchema)

        case IndexerStartupMode.ValidateAndStart =>
          flywayMigrations.validate()

        case IndexerStartupMode.ValidateAndWaitOnly(
              schemaMigrationAttempts,
              schemaMigrationAttemptBackoff,
            ) =>
          flywayMigrations
            .validateAndWaitOnly(schemaMigrationAttempts, schemaMigrationAttemptBackoff)

        case IndexerStartupMode.MigrateOnEmptySchemaAndStart =>
          flywayMigrations.migrateOnEmptySchema()
      }
    )
}

object NoOpKillSwitch extends KillSwitch {
  override def shutdown(): Unit = ()

  override def abort(ex: Throwable): Unit = ()
}
