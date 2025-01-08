// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.DomainIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.DomainOffset
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.{DbSupport, FlywayMigrations}
import com.digitalasset.canton.platform.{ResourceCloseable, ResourceOwnerFlagCloseableOps}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LedgerParticipantId, config}

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class LedgerApiStore(
    val ledgerApiDbSupport: DbSupport,
    val ledgerApiStorage: LedgerApiStorage,
    val ledgerEndCache: MutableLedgerEndCache,
    val stringInterningView: StringInterningView,
    val metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
) extends ResourceCloseable {
  private val integrityStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createIntegrityStorageBackend
  private val parameterStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createParameterStorageBackend(stringInterningView)
  private val eventStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createEventStorageBackend(
      ledgerEndCache,
      stringInterningView,
      loggerFactory,
    )
  private val stringInterningStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createStringInterningStorageBackend

  private def executeSql[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit traceContext: TraceContext): Future[T] =
    ledgerApiDbSupport.dbDispatcher.executeSql(databaseMetrics)(sql)(
      new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
    )

  private def executeSqlUS[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown.outcomeF(
      ledgerApiDbSupport.dbDispatcher.executeSql(databaseMetrics)(sql)(
        new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
      )
    )

  def onlyForTestingVerifyIntegrity(failForEmptyDB: Boolean = true)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    executeSqlUS(DatabaseMetrics.ForTesting("checkIntegrity"))(
      integrityStorageBackend.onlyForTestingVerifyIntegrity(failForEmptyDB)
    )

  def onlyForTestingMoveLedgerEndBackToScratch()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    executeSqlUS(DatabaseMetrics.ForTesting("onlyForTestingMoveLedgerEndBackToScratch"))(
      integrityStorageBackend.onlyForTestingMoveLedgerEndBackToScratch()
    )

  def onlyForTestingNumberOfAcceptedTransactionsFor(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Int] =
    executeSqlUS(DatabaseMetrics.ForTesting("numberOfAcceptedTransactionsFor"))(
      integrityStorageBackend.onlyForTestingNumberOfAcceptedTransactionsFor(synchronizerId)
    )

  def cleanDomainIndex(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainIndex]] =
    executeSqlUS(metrics.index.db.getCleanDomainIndex)(
      parameterStorageBackend.cleanDomainIndex(synchronizerId)
    )

  def ledgerEnd(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[LedgerEnd]] =
    executeSqlUS(metrics.index.db.getLedgerEnd)(
      parameterStorageBackend.ledgerEnd
    )

  def topologyEventPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    executeSql(metrics.index.db.getTopologyEventPublishedOnRecordTime)(
      eventStorageBackend.topologyEventPublishedOnRecordTime(synchronizerId, recordTime)
    )

  def firstDomainOffsetAfterOrAt(
      synchronizerId: SynchronizerId,
      afterOrAtRecordTimeInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.firstDomainOffsetAfterOrAt)(
      eventStorageBackend.firstDomainOffsetAfterOrAt(
        synchronizerId,
        afterOrAtRecordTimeInclusive.underlying,
      )
    )

  def lastDomainOffsetBeforeOrAt(
      synchronizerId: SynchronizerId,
      beforeOrAtOffsetInclusive: Offset,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.lastDomainOffsetBeforeOrAt)(
      eventStorageBackend.lastDomainOffsetBeforeOrAt(
        Some(synchronizerId),
        beforeOrAtOffsetInclusive,
      )
    )

  def lastDomainOffsetBeforeOrAt(
      beforeOrAtOffsetInclusive: Offset
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.lastDomainOffsetBeforeOrAt)(
      eventStorageBackend.lastDomainOffsetBeforeOrAt(None, beforeOrAtOffsetInclusive)
    )

  def domainOffset(offset: Offset)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.domainOffset)(
      eventStorageBackend.domainOffset(offset)
    )

  def firstDomainOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.firstDomainOffsetAfterOrAtPublicationTime)(
      eventStorageBackend.firstDomainOffsetAfterOrAtPublicationTime(
        afterOrAtPublicationTimeInclusive.underlying
      )
    )

  def lastDomainOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[DomainOffset]] =
    executeSqlUS(metrics.index.db.lastDomainOffsetBeforeOrAtPublicationTime)(
      eventStorageBackend.lastDomainOffsetBeforeOrAtPublicationTime(
        beforeOrAtPublicationTimeInclusive.underlying
      )
    )

  def archivals(fromExclusive: Option[Offset], toInclusive: Offset)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Set[LfContractId]] =
    executeSqlUS(metrics.index.db.archivals)(
      eventStorageBackend.archivals(fromExclusive, toInclusive)
    )

  private[api] def initializeInMemoryState(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    for {
      currentLedgerEnd <- ledgerEnd
      _ <- FutureUnlessShutdown.outcomeF(
        stringInterningView.update(
          currentLedgerEnd.map(_.lastStringInterningId)
        )((fromExclusive, toInclusive) =>
          executeSql(metrics.index.db.loadStringInterningEntries)(
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          )
        )
      )
    } yield {
      ledgerEndCache.set(currentLedgerEnd)
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
      onlyForTesting_DoNotInitializeInMemoryState: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContextIdlenessExecutorService,
  ): FutureUnlessShutdown[LedgerApiStore] = {
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
        case _: StorageConfig.Memory =>
          FutureUnlessShutdown.outcomeF(
            new FlywayMigrations(
              ledgerApiStorage.jdbcUrl,
              loggerFactory,
            ).migrate()
          )
        case _ => FutureUnlessShutdown.unit
      }
      dbSupport = DbSupport
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

      ledgerApiStore <- FutureUnlessShutdown.outcomeF(
        new ResourceOwnerFlagCloseableOps(dbSupport).acquireFlagCloseable(
          "Ledger API DB Support"
        )
      )
      _ <-
        if (onlyForTesting_DoNotInitializeInMemoryState) FutureUnlessShutdown.unit
        else ledgerApiStore.initializeInMemoryState
    } yield ledgerApiStore
  }
}
