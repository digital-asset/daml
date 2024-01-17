// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.{
  EventLogId,
  MultiDomainEventLogTest,
  SerializableLedgerSyncEvent,
  TransferStore,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.{DbStorage, IdempotentInsert}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.tracing.SerializableTraceContext
import slick.dbio.DBIOAction
import slick.jdbc.SetParameter

import java.util.concurrent.Semaphore
import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, blocking}

trait DbMultiDomainEventLogTest extends MultiDomainEventLogTest with DbTest {

  override def beforeAll(): Unit = {
    DbMultiDomainEventLogTest.acquireLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
    super.beforeAll()
  }

  override def afterAll() = {
    super.afterAll()
    DbMultiDomainEventLogTest.releaseLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
  }

  override protected def transferStores: Map[TargetDomainId, TransferStore] = domainIds.view.map {
    domainId =>
      val targetDomainId = TargetDomainId(domainId)
      val transferStore = new DbTransferStore(
        storage,
        targetDomainId,
        testedProtocolVersion,
        TestingIdentityFactory.pureCrypto(),
        futureSupervisor,
        timeouts,
        loggerFactory,
      )

      targetDomainId -> transferStore
  }.toMap

  // If this test is run multiple times against a local persisted Postgres DB,
  // then the second run would find the requests from the first run and fail.
  override protected def cleanUpEventLogs(): Unit = {
    val theStorage = storage
    import theStorage.api.*

    val cleanupF = theStorage.update(
      DBIO.seq(
        sqlu"truncate table linearized_event_log", // table guarded by DbMultiDomainEventLogTest.acquireLinearizedEventLogLock
        sqlu"delete from event_log where ${indexedStringStore.minIndex} <= log_id and log_id <= ${indexedStringStore.maxIndex}", // table shared with other tests
        sqlu"delete from event_log where log_id = $participantEventLogId", // table shared with other tests
      ),
      functionFullName,
    )
    Await.result(cleanupF, 10.seconds)
  }

  override def cleanDb(storage: DbStorage): Future[_] =
    Future.unit // Don't delete anything between tests, as tests depend on each other.

  override def storeEventsToSingleDimensionEventLogs(
      events: Seq[(EventLogId, TimestampedEvent)]
  ): Future[Unit] = {
    val theStorage = storage
    import theStorage.api.*
    import theStorage.converters.*

    @nowarn("cat=unused") implicit val setParameterTraceContext
        : SetParameter[SerializableTraceContext] =
      SerializableTraceContext.getVersionedSetParameter(testedProtocolVersion)
    @nowarn("cat=unused") implicit val setParameterSerializableLedgerSyncEvent
        : SetParameter[SerializableLedgerSyncEvent] =
      SerializableLedgerSyncEvent.getVersionedSetParameter

    val queries = events.map {
      case (id, tsEvent @ TimestampedEvent(event, localOffset, requestSequencerCounter, eventId)) =>
        val serializableLedgerSyncEvent = SerializableLedgerSyncEvent(event, testedProtocolVersion)

        IdempotentInsert.insertIgnoringConflicts(
          storage,
          "event_log pk_event_log",
          sql"""event_log (log_id, local_offset_effective_time, local_offset_tie_breaker, local_offset_discriminator, ts, request_sequencer_counter, event_id, content, trace_context)
               values (${id.index}, ${localOffset.effectiveTime}, ${localOffset.tieBreaker}, ${localOffset.discriminator},
               ${tsEvent.timestamp}, $requestSequencerCounter, $eventId, $serializableLedgerSyncEvent,
                 ${SerializableTraceContext(tsEvent.traceContext)})""",
        )
    }

    theStorage.update_(DBIOAction.sequence(queries), functionFullName)
  }

  private def createDbMultiDomainEventLog(
      storage: DbStorage,
      clock: Clock,
  ): Future[DbMultiDomainEventLog] = {
    DbMultiDomainEventLog.apply(
      storage,
      clock,
      ParticipantTestMetrics,
      DefaultProcessingTimeouts.testing,
      indexedStringStore,
      loggerFactory,
      maxBatchSize = PositiveInt.tryCreate(3),
      participantEventLogId = participantEventLogId,
      transferStoreFor = domainId =>
        transferStores.get(domainId).toRight(s"Cannot find transfer store for domain $domainId"),
    )
  }

  "DbMultiDomainEventLog" should {
    behave like multiDomainEventLog { clock =>
      Await.result(createDbMultiDomainEventLog(storage, clock), 10.seconds)
    }
  }
}

private object DbMultiDomainEventLogTest {

  /** Synchronize access to the linearized_event_log so that tests do not interfere */
  private val accessLinearizedEventLog: Semaphore = new Semaphore(1)

  def acquireLinearizedEventLogLock(elc: ErrorLoggingContext): Unit = {
    elc.logger.info(s"Acquiring linearized event log test lock")(elc.traceContext)
    blocking(accessLinearizedEventLog.acquire())
  }

  def releaseLinearizedEventLogLock(elc: ErrorLoggingContext): Unit = {
    elc.logger.info(s"Releasing linearized event log test lock")(elc.traceContext)
    accessLinearizedEventLog.release()
  }
}

class MultiDomainEventLogTestH2 extends DbMultiDomainEventLogTest with H2Test

class MultiDomainEventLogTestPostgres extends DbMultiDomainEventLogTest with PostgresTest
