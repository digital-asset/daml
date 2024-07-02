// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{MemoryStorageConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.ResourceCloseable
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.{DbSupport, FlywayMigrations}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LedgerParticipantId, config}

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

final class LedgerApiStore(
    val ledgerApiDbSupport: DbSupport,
    val ledgerApiStorage: LedgerApiStorage,
    val ledgerEndCache: MutableLedgerEndCache,
    val stringInterningView: StringInterningView,
    val metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
) extends ResourceCloseable {
  private val parameterStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createParameterStorageBackend
  private val stringInterningStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createStringInterningStorageBackend

  private def executeSql[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit traceContext: TraceContext): Future[T] =
    ledgerApiDbSupport.dbDispatcher.executeSql(databaseMetrics)(sql)(
      new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
    )

  def ledgerEnd(implicit traceContext: TraceContext): Future[LedgerEnd] =
    executeSql(metrics.index.db.getLedgerEnd)(
      parameterStorageBackend.ledgerEnd
    )

  private[api] def initializeInMemoryState(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Unit] =
    for {
      currentLedgerEnd <- ledgerEnd
      _ <- stringInterningView.update(currentLedgerEnd.lastStringInterningId)(
        (fromExclusive, toInclusive) =>
          executeSql(metrics.index.db.loadStringInterningEntries)(
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          )
      )
    } yield {
      ledgerEndCache.set(
        (
          currentLedgerEnd.lastOffset,
          currentLedgerEnd.lastEventSeqId,
        )
      )
    }
}

object LedgerApiStore {
  def initialize(
      storageConfig: StorageConfig,
      ledgerParticipantId: LedgerParticipantId,
      legderApiDatabaseConnectionTimeout: config.NonNegativeFiniteDuration,
      ledgerApiPostgresDataSourceConfig: PostgresDataSourceConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContextIdlenessExecutorService,
  ): Future[LedgerApiStore] = {
    val initializationLogger = loggerFactory.getTracedLogger(LedgerApiStore.getClass)
    val ledgerApiStorage = LedgerApiStorage
      .fromStorageConfig(storageConfig, ledgerParticipantId)
      .fold(
        error => throw new IllegalStateException(s"Constructing LedgerApiStorage failed: $error"),
        identity,
      )
    val dbConfig = DbSupport.DbConfig(
      jdbcUrl = ledgerApiStorage.jdbcUrl,
      connectionPool = DbSupport.ConnectionPoolConfig(
        connectionPoolSize = storageConfig.numConnectionsLedgerApiServer.unwrap,
        connectionTimeout = legderApiDatabaseConnectionTimeout.underlying,
      ),
      postgres = ledgerApiPostgresDataSourceConfig,
    )
    val numLedgerApi = dbConfig.connectionPool.connectionPoolSize
    initializationLogger.info(s"Creating ledger API storage num-ledger-api: $numLedgerApi")

    for {
      _ <- storageConfig match {
        // ledger api server needs an H2 db to run in memory
        case _: MemoryStorageConfig =>
          new FlywayMigrations(
            ledgerApiStorage.jdbcUrl,
            loggerFactory,
          ).migrate()
        case _ => Future.unit
      }
      ledgerApiStore <- DbSupport
        .owner(
          serverRole = ServerRole.ApiServer,
          metrics = metrics,
          dbConfig = dbConfig,
          loggerFactory = loggerFactory,
        )
        .map(dbSupport =>
          new LedgerApiStore(
            ledgerApiDbSupport = dbSupport,
            ledgerApiStorage = ledgerApiStorage,
            ledgerEndCache = MutableLedgerEndCache(),
            stringInterningView = new StringInterningView(loggerFactory),
            metrics = metrics,
            loggerFactory = loggerFactory,
            timeouts = timeouts,
          )
        )
        .acquireFlagCloseable("Ledger API DB Support")
      _ <- ledgerApiStore.initializeInMemoryState
    } yield ledgerApiStore
  }
}
