// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.Update.{
  RepairTransactionAccepted,
  TopologyTransactionEffective,
}
import com.digitalasset.canton.ledger.participant.state.{
  RepairIndex,
  SequencerIndex,
  SynchronizerIndex,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule.LoggerNameContains
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLogging,
  SuppressingLogger,
  SuppressionRule,
  TracedLogger,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.indexer.ha.TestConnection
import com.digitalasset.canton.platform.indexer.parallel.ParallelIndexerSubscription.{
  Batch,
  ZeroLedgerEnd,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{DbDto, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value.ContractId
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.slf4j.event.Level

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class ParallelIndexerSubscriptionSpec
    extends AnyFlatSpec
    with ScalaFutures
    with Matchers
    with NamedLogging {

  implicit val traceContext: TraceContext = TraceContext.empty
  private val serializableTraceContext =
    SerializableTraceContext(traceContext).toDamlProto.toByteArray
  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  implicit val actorSystem: ActorSystem = ActorSystem(
    classOf[ParallelIndexerSubscriptionSpec].getSimpleName
  )
  implicit val materializer: Materializer = Materializer(actorSystem)

  private val someParty = DbDto.PartyEntry(
    ledger_offset = 1,
    recorded_at = 0,
    submission_id = null,
    party = Some("party"),
    typ = "accept",
    rejection_reason = None,
    is_local = Some(true),
  )

  private val someSynchronizerId: SynchronizerId = SynchronizerId.tryFromString("x::synchronizerId")
  private val someSynchronizerId2: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizerId2")
  private val someSynchronizerId3: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizerId3")

  private val someTime = Instant.now

  private val somePartyAllocation = state.Update.PartyAddedToParticipant(
    party = Ref.Party.assertFromString("party"),
    participantId = Ref.ParticipantId.assertFromString("participant"),
    recordTime = CantonTimestamp.assertFromInstant(someTime),
    submissionId = Some(Ref.SubmissionId.assertFromString("abc")),
  )

  private def offset(l: Long): Offset = Offset.tryFromLong(l)

  private val metrics = LedgerApiServerMetrics.ForTesting

  private def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  private val someEventCreated = DbDto.EventCreate(
    event_offset = 1,
    update_id = "",
    ledger_effective_time = 15,
    command_id = None,
    workflow_id = None,
    user_id = None,
    submitters = None,
    node_id = 3,
    contract_id = hashCid("1").toBytes.toByteArray,
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    tree_event_witnesses = Set.empty,
    create_argument = Array.empty,
    create_signatories = Set.empty,
    create_observers = Set.empty,
    create_key_value = None,
    create_key_maintainers = None,
    create_key_hash = None,
    create_argument_compression = None,
    create_key_value_compression = None,
    event_sequential_id = 0,
    driver_metadata = Array.empty,
    synchronizer_id = "x::sourcesynchronizer",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventExercise = DbDto.EventExercise(
    consuming = true,
    event_offset = 1,
    update_id = "",
    ledger_effective_time = 15,
    command_id = None,
    workflow_id = None,
    user_id = None,
    submitters = None,
    node_id = 3,
    contract_id = hashCid("1").toBytes.toByteArray,
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    tree_event_witnesses = Set.empty,
    create_key_value = None,
    exercise_choice = "",
    exercise_argument = Array.empty,
    exercise_result = None,
    exercise_actors = Set.empty,
    exercise_last_descendant_node_id = 3,
    create_key_value_compression = None,
    exercise_argument_compression = None,
    exercise_result_compression = None,
    event_sequential_id = 0,
    synchronizer_id = "",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventAssign = DbDto.EventAssign(
    event_offset = 1,
    update_id = "",
    command_id = None,
    workflow_id = None,
    submitter = None,
    node_id = 0,
    contract_id = hashCid("1").toBytes.toByteArray,
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    create_argument = Array.empty,
    create_signatories = Set.empty,
    create_observers = Set.empty,
    create_key_value = None,
    create_key_maintainers = None,
    create_key_hash = None,
    create_argument_compression = None,
    create_key_value_compression = None,
    event_sequential_id = 0,
    ledger_effective_time = 0,
    driver_metadata = Array.empty,
    source_synchronizer_id = "",
    target_synchronizer_id = "",
    unassign_id = "",
    reassignment_counter = 0,
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventUnassign = DbDto.EventUnassign(
    event_offset = 1,
    update_id = "",
    command_id = None,
    workflow_id = None,
    submitter = None,
    node_id = 1,
    contract_id = hashCid("1").toBytes.toByteArray,
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    event_sequential_id = 0,
    source_synchronizer_id = "",
    target_synchronizer_id = "",
    unassign_id = "",
    reassignment_counter = 0,
    assignment_exclusivity = None,
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someCompletion = DbDto.CommandCompletion(
    completion_offset = 1,
    record_time = 0,
    publication_time = 0,
    user_id = "",
    submitters = Set.empty,
    command_id = "",
    update_id = None,
    rejection_status_code = None,
    rejection_status_message = None,
    rejection_status_details = None,
    submission_id = None,
    deduplication_offset = None,
    deduplication_duration_seconds = None,
    deduplication_duration_nanos = None,
    synchronizer_id = "x::sourcesynchronizer",
    message_uuid = None,
    is_transaction = true,
    trace_context = serializableTraceContext,
  )

  private val offsetsAndUpdates =
    Vector(1L, 2L, 3L)
      .map(offset)
      .zip(
        Vector(
          somePartyAllocation,
          somePartyAllocation.copy(recordTime = somePartyAllocation.recordTime.addMicros(1000)),
          somePartyAllocation.copy(recordTime = somePartyAllocation.recordTime.addMicros(2000)),
        )
      )

  behavior of "inputMapper"

  it should "provide required Batch in happy path case" in {
    val actual = ParallelIndexerSubscription.inputMapper(
      metrics = metrics,
      toDbDto = _ => _ => Iterator(someParty, someParty),
      eventMetricsUpdater = _ => (),
      logger,
    )(
      List(
        Offset.tryFromLong(1),
        Offset.tryFromLong(2),
        Offset.tryFromLong(3),
      ).zip(offsetsAndUpdates.map(_._2))
    )
    val expected = Batch[Vector[DbDto]](
      ledgerEnd = LedgerEnd(
        lastOffset = offset(3),
        lastEventSeqId = 0L,
        lastStringInterningId = 0,
        lastPublicationTime = CantonTimestamp.MinValue,
      ),
      lastTraceContext = TraceContext.empty,
      batch = Vector(
        someParty,
        someParty,
        someParty,
        someParty,
        someParty,
        someParty,
      ),
      batchSize = 3,
      offsetsUpdates = offsetsAndUpdates,
    )
    actual shouldBe expected
  }

  behavior of "seqMapperZero"

  it should "provide required Batch in happy path case" in {
    val ledgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 123,
      lastStringInterningId = 234,
      lastPublicationTime = CantonTimestamp.now(),
    )

    ParallelIndexerSubscription.seqMapperZero(Some(ledgerEnd)) shouldBe Batch(
      ledgerEnd = ledgerEnd,
      lastTraceContext = TraceContext.empty,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )
  }

  it should "provide required Batch in case starting from scratch" in {
    ParallelIndexerSubscription.seqMapperZero(None) shouldBe Batch(
      ledgerEnd = ZeroLedgerEnd,
      lastTraceContext = TraceContext.empty,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )
  }

  behavior of "seqMapper"

  it should "assign sequence ids correctly, and populate string-interning entries correctly in happy path case" in {
    val clockStart = CantonTimestamp.now()
    val simClock = new SimClock(clockStart, loggerFactory)

    val previousPublicationTime = simClock.monotonicTime()
    val currentPublicationTime = simClock.uniqueTime()
    previousPublicationTime should not be currentPublicationTime
    val previousLedgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 15,
      lastStringInterningId = 26,
      lastPublicationTime = previousPublicationTime,
    )
    val result = ParallelIndexerSubscription.seqMapper(
      internize = _.zipWithIndex.map(x => x._2 -> x._2.toString).take(2),
      metrics,
      simClock,
      logger,
    )(
      previous = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
      current = Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        lastTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someEventCreated,
          DbDto.IdFilterCreateStakeholder(0L, "", ""),
          DbDto.IdFilterCreateNonStakeholderInformee(0L, "", ""),
          DbDto.IdFilterConsumingStakeholder(0L, "", ""),
          DbDto.IdFilterConsumingNonStakeholderInformee(0L, "", ""),
          DbDto.IdFilterNonConsumingInformee(0L, "", ""),
          someEventCreated,
          someEventCreated,
          DbDto.TransactionMeta("", 1, 0L, 0L, "x::sourcesynchronizer", 0L, 0L),
          someParty,
          someEventExercise,
          DbDto.TransactionMeta("", 1, 0L, 0L, "x::sourcesynchronizer", 0L, 0L),
          someParty,
          someEventAssign,
          DbDto.IdFilterAssignStakeholder(0L, "", ""),
          DbDto.IdFilterAssignStakeholder(0L, "", ""),
          DbDto.TransactionMeta("", 1, 0L, 0L, "x::sourcesynchronizer", 0L, 0L),
          someParty,
          someEventUnassign,
          DbDto.IdFilterUnassignStakeholder(0L, "", ""),
          DbDto.IdFilterUnassignStakeholder(0L, "", ""),
          DbDto.TransactionMeta("", 1, 0L, 0L, "x::sourcesynchronizer", 0L, 0L),
          someParty,
          someCompletion,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
      ),
    )
    import scala.util.chaining.*

    result.ledgerEnd.lastEventSeqId shouldBe 21
    result.ledgerEnd.lastStringInterningId shouldBe 1
    result.ledgerEnd.lastPublicationTime shouldBe currentPublicationTime
    result.ledgerEnd.lastOffset shouldBe offset(2)
    result.batch(2).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 16
    result.batch(3).asInstanceOf[DbDto.IdFilterCreateStakeholder].event_sequential_id shouldBe 16
    result
      .batch(4)
      .asInstanceOf[DbDto.IdFilterCreateNonStakeholderInformee]
      .event_sequential_id shouldBe 16
    result.batch(5).asInstanceOf[DbDto.IdFilterConsumingStakeholder].event_sequential_id shouldBe 16
    result
      .batch(6)
      .asInstanceOf[DbDto.IdFilterConsumingNonStakeholderInformee]
      .event_sequential_id shouldBe 16
    result.batch(7).asInstanceOf[DbDto.IdFilterNonConsumingInformee].event_sequential_id shouldBe 16
    result.batch(10).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 16L
      transactionMeta.event_sequential_id_last shouldBe 18L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result.batch(12).asInstanceOf[DbDto.EventExercise].event_sequential_id shouldBe 19
    result.batch(13).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 19L
      transactionMeta.event_sequential_id_last shouldBe 19L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result.batch(15).asInstanceOf[DbDto.EventAssign].event_sequential_id shouldBe 20L
    result.batch(16).asInstanceOf[DbDto.IdFilterAssignStakeholder].event_sequential_id shouldBe 20L
    result.batch(17).asInstanceOf[DbDto.IdFilterAssignStakeholder].event_sequential_id shouldBe 20L
    result.batch(18).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 20L
      transactionMeta.event_sequential_id_last shouldBe 20L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result.batch(20).asInstanceOf[DbDto.EventUnassign].event_sequential_id shouldBe 21L
    result
      .batch(21)
      .asInstanceOf[DbDto.IdFilterUnassignStakeholder]
      .event_sequential_id shouldBe 21L
    result
      .batch(22)
      .asInstanceOf[DbDto.IdFilterUnassignStakeholder]
      .event_sequential_id shouldBe 21L
    result.batch(23).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 21L
      transactionMeta.event_sequential_id_last shouldBe 21L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result
      .batch(25)
      .asInstanceOf[DbDto.CommandCompletion]
      .publication_time shouldBe currentPublicationTime.toMicros
    result.batch(26).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 0
    result.batch(26).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "0"
    result.batch(27).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 1
    result.batch(27).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "1"
  }

  it should "preserve sequence id if nothing to assign" in {
    val previousLedgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 15,
      lastStringInterningId = 25,
      lastPublicationTime = CantonTimestamp.now(),
    )
    val simClock = new SimClock(loggerFactory = loggerFactory)
    val result = ParallelIndexerSubscription.seqMapper(_ => Nil, metrics, simClock, logger)(
      ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
      Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        lastTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someParty,
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
      ),
    )
    result.ledgerEnd.lastEventSeqId shouldBe 15
    result.ledgerEnd.lastStringInterningId shouldBe 25
    result.ledgerEnd.lastOffset shouldBe offset(2)
  }

  it should "take the last publication time, if bigger than the current time, and log" in {
    val now = CantonTimestamp.now()
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val previous = now.plusSeconds(10)
    val previousLedgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 15,
      lastStringInterningId = 25,
      lastPublicationTime = previous,
    )
    loggerFactory.assertLogs(
      LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(Level.INFO)
    )(
      ParallelIndexerSubscription
        .seqMapper(_ => Nil, metrics, simClock, logger)(
          ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
          Batch(
            ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
            lastTraceContext = TraceContext.empty,
            batch = Vector(
              someParty,
              someParty,
              someParty,
              someParty,
            ),
            batchSize = 3,
            offsetsUpdates = offsetsAndUpdates,
          ),
        )
        .ledgerEnd
        .lastPublicationTime shouldBe previous,
      _.infoMessage should include("Has the clock been reset, e.g., during participant failover?"),
    )
  }

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerSubscription.batcher(
      batchF = _ => "bumm"
    )(
      Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        lastTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someParty,
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
      )
    )
    result shouldBe Batch(
      ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
      lastTraceContext = TraceContext.empty,
      batch = "bumm",
      batchSize = 3,
      offsetsUpdates = offsetsAndUpdates,
    )
  }

  behavior of "ingester"

  it should "apply ingestFunction and cleanUnusedBatch" in {
    val connection = new TestConnection
    val dbDispatcher = new DbDispatcher {
      override def executeSql[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[T] =
        Future.successful(sql(connection))

      override val executor: QueueAwareExecutor & NamedExecutor = new QueueAwareExecutor
        with NamedExecutor {
        override def queueSize: Long = 0
        override def name: String = "test"
      }

      override def executeSqlUS[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
          loggingContext: LoggingContextWithTrace,
          ec: ExecutionContext,
      ): FutureUnlessShutdown[T] =
        FutureUnlessShutdown.pure(sql(connection))
    }

    val batchPayload = "Some batch payload"

    val ingestFunction: (Connection, String) => Unit = {
      case (`connection`, `batchPayload`) => ()
      case other => fail(s"Unexpected: $other")
    }

    val ledgerEnd = LedgerEnd(
      lastOffset = offset(2),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
      lastPublicationTime = CantonTimestamp.MinValue,
    )
    val inBatch = Batch(
      ledgerEnd = ledgerEnd,
      lastTraceContext = TraceContext.empty,
      batch = batchPayload,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

    val persistedTransferOffsets = new AtomicBoolean(false)
    val zeroDbBatch = "zero"
    val outBatchF =
      ParallelIndexerSubscription.ingester(
        ingestFunction,
        new ReassignmentOffsetPersistence {
          override def persist(updates: Seq[(Offset, Update)], tracedLogger: TracedLogger)(implicit
              traceContext: TraceContext
          ): Future[Unit] = {
            persistedTransferOffsets.set(true)
            Future.unit
          }
        },
        "zero",
        dbDispatcher,
        metrics,
        logger,
      )(
        traceContext
      )(inBatch)

    val outBatch = Await.result(outBatchF, 10.seconds)

    outBatch shouldBe
      Batch(
        ledgerEnd = ledgerEnd,
        lastTraceContext = TraceContext.empty,
        batch = zeroDbBatch,
        batchSize = 0,
        offsetsUpdates = Vector.empty,
      )
    persistedTransferOffsets.get() shouldBe true
  }

  behavior of "ingestTail"

  it should "apply ingestTailFunction on the last batch and forward the batch of batches" in {
    val ledgerEnd = ParameterStorageBackend.LedgerEnd(
      lastOffset = offset(5),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
      lastPublicationTime = CantonTimestamp.MinValue,
    )

    val secondBatchLedgerEnd = ParameterStorageBackend.LedgerEnd(
      lastOffset = offset(6),
      lastEventSeqId = 3000,
      lastStringInterningId = 400,
      lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
    )

    val storeLedgerEndF: (LedgerEnd, Map[SynchronizerId, SynchronizerIndex]) => Future[Unit] = {
      case (`secondBatchLedgerEnd`, _) => Future.unit
      case otherLedgerEnd => fail(s"Unexpected ledger end: $otherLedgerEnd")
    }

    val batch = Batch(
      ledgerEnd = ledgerEnd,
      lastTraceContext = TraceContext.empty,
      batch = "Some batch payload",
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

    val batchOfBatches = Vector(
      batch,
      batch.copy(ledgerEnd = secondBatchLedgerEnd),
    )

    val outBatchF =
      ParallelIndexerSubscription.ingestTail(
        storeLedgerEndF,
        logger,
      )(
        traceContext
      )(batchOfBatches)

    val outBatch = Await.result(outBatchF, 10.seconds)
    outBatch shouldBe batchOfBatches
  }

  behavior of "synchronizerLedgerEndFromBatch"

  private val someSequencerIndex1 = SequencerIndex(
    sequencerTimestamp = CantonTimestamp.ofEpochMicro(123)
  )
  private val someSequencerIndex2 = SequencerIndex(
    sequencerTimestamp = CantonTimestamp.ofEpochMicro(256)
  )
  private val someRepairIndex1 = RepairIndex(
    timestamp = CantonTimestamp.ofEpochMicro(153),
    counter = RepairCounter(15),
  )
  private val someRepairIndex2 = RepairIndex(
    timestamp = CantonTimestamp.ofEpochMicro(156),
    counter = RepairCounter.Genesis,
  )
  private val someRecordTime1 = CantonTimestamp.ofEpochMicro(100)
  private val someRecordTime2 = CantonTimestamp.ofEpochMicro(300)
  private val someRepairCounter1 = RepairCounter(0)

  it should "populate correct ledger-end from batches for a sequencer counter moved" in {
    ParallelIndexerSubscription.ledgerEndSynchronizerIndexFrom(
      Vector(someSynchronizerId -> SynchronizerIndex.of(someSequencerIndex1))
    ) shouldBe Map(
      someSynchronizerId -> SynchronizerIndex.of(someSequencerIndex1)
    )
  }

  it should "populate correct ledger-end from batches for a repair counter moved" in {
    ParallelIndexerSubscription.ledgerEndSynchronizerIndexFrom(
      Vector(someSynchronizerId -> SynchronizerIndex.of(someRepairIndex1))
    ) shouldBe Map(
      someSynchronizerId -> SynchronizerIndex.of(someRepairIndex1)
    )
  }

  it should "populate correct ledger-end from batches for a mixed batch" in {
    ParallelIndexerSubscription.ledgerEndSynchronizerIndexFrom(
      Vector(
        someSynchronizerId -> SynchronizerIndex.of(someSequencerIndex1),
        someSynchronizerId -> SynchronizerIndex.of(
          RepairIndex(someRecordTime1, someRepairCounter1)
        ),
        someSynchronizerId -> SynchronizerIndex.of(someSequencerIndex2),
        someSynchronizerId2 -> SynchronizerIndex.of(someRepairIndex1),
        someSynchronizerId2 -> SynchronizerIndex.of(someRepairIndex2),
        someSynchronizerId2 -> SynchronizerIndex.of(someRecordTime1),
      )
    ) shouldBe Map(
      someSynchronizerId -> SynchronizerIndex(
        Some(RepairIndex(someRecordTime1, someRepairCounter1)),
        Some(someSequencerIndex2),
        someSequencerIndex2.sequencerTimestamp,
      ),
      someSynchronizerId2 -> SynchronizerIndex(
        Some(someRepairIndex2),
        None,
        someRepairIndex2.timestamp,
      ),
    )
  }

  it should "populate correct ledger-end from batches for a mixed batch 2" in {
    ParallelIndexerSubscription.ledgerEndSynchronizerIndexFrom(
      Vector(
        someSynchronizerId -> SynchronizerIndex.of(someSequencerIndex1),
        someSynchronizerId -> SynchronizerIndex.of(someRepairIndex1),
        someSynchronizerId -> SynchronizerIndex.of(someRecordTime2),
        someSynchronizerId2 -> SynchronizerIndex.of(someSequencerIndex1),
        someSynchronizerId2 -> SynchronizerIndex.of(someRepairIndex2),
        someSynchronizerId3 -> SynchronizerIndex.of(someSequencerIndex1),
        someSynchronizerId3 -> SynchronizerIndex.of(
          RepairIndex(someSequencerIndex1.sequencerTimestamp, RepairCounter.Genesis)
        ),
      )
    ) shouldBe Map(
      someSynchronizerId -> SynchronizerIndex(
        Some(someRepairIndex1),
        Some(someSequencerIndex1),
        someRecordTime2,
      ),
      someSynchronizerId2 -> SynchronizerIndex(
        Some(someRepairIndex2),
        Some(someSequencerIndex1),
        someRepairIndex2.timestamp,
      ),
      someSynchronizerId3 -> SynchronizerIndex(
        Some(RepairIndex(someSequencerIndex1.sequencerTimestamp, RepairCounter.Genesis)),
        Some(someSequencerIndex1),
        someSequencerIndex1.sequencerTimestamp,
      ),
    )
  }

  behavior of "aggregateLedgerEndForRepair"

  private val someAggregatedLedgerEndForRepair
      : Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])] =
    Some(
      ParameterStorageBackend.LedgerEnd(
        lastOffset = offset(5),
        lastEventSeqId = 2000,
        lastStringInterningId = 300,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(5),
      ) -> Map(
        someSynchronizerId -> SynchronizerIndex(
          None,
          Some(
            SequencerIndex(
              sequencerTimestamp = CantonTimestamp.ofEpochMicro(5)
            )
          ),
          CantonTimestamp.ofEpochMicro(5),
        ),
        someSynchronizerId2 -> SynchronizerIndex(
          Some(someRepairIndex2),
          Some(
            SequencerIndex(
              sequencerTimestamp = CantonTimestamp.ofEpochMicro(4)
            )
          ),
          CantonTimestamp.ofEpochMicro(4),
        ),
      )
    )

  private val someBatchOfBatches: Vector[Batch[Unit]] = Vector(
    Batch(
      ledgerEnd = LedgerEnd(
        lastOffset = offset(10),
        lastEventSeqId = 2010,
        lastStringInterningId = 310,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(15),
      ),
      lastTraceContext = TraceContext.empty,
      batch = (),
      batchSize = 0,
      offsetsUpdates = Vector(
        offset(9) ->
          Update.SequencerIndexMoved(
            synchronizerId = someSynchronizerId,
            recordTime = someSequencerIndex1.sequencerTimestamp,
          ),
        offset(10) ->
          Update.SequencerIndexMoved(
            synchronizerId = someSynchronizerId2,
            recordTime = someSequencerIndex1.sequencerTimestamp,
          ),
      ),
    ),
    Batch(
      ledgerEnd = LedgerEnd(
        lastOffset = offset(20),
        lastEventSeqId = 2020,
        lastStringInterningId = 320,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
      ),
      lastTraceContext = TraceContext.empty,
      batch = (),
      batchSize = 0,
      offsetsUpdates = Vector(
        offset(19) ->
          Update.SequencerIndexMoved(
            synchronizerId = someSynchronizerId,
            recordTime = someSequencerIndex2.sequencerTimestamp,
          ),
        offset(20) ->
          Update.SequencerIndexMoved(
            synchronizerId = someSynchronizerId2,
            recordTime = someSequencerIndex2.sequencerTimestamp,
          ),
      ),
    ),
  )

  it should "correctly aggregate if batch has no new synchronizer-indexes" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]](
        someAggregatedLedgerEndForRepair
      )
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(Vector.empty)
    aggregateLedgerEndForRepairRef.get() shouldBe someAggregatedLedgerEndForRepair
  }

  it should "correctly aggregate if old state is empty" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]](None)
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(someBatchOfBatches)
    aggregateLedgerEndForRepairRef.get() shouldBe
      Some(
        ParameterStorageBackend.LedgerEnd(
          lastOffset = offset(20),
          lastEventSeqId = 2020,
          lastStringInterningId = 320,
          lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
        ) -> Map(
          someSynchronizerId -> SynchronizerIndex(
            None,
            Some(someSequencerIndex2),
            someSequencerIndex2.sequencerTimestamp,
          ),
          someSynchronizerId2 -> SynchronizerIndex(
            None,
            Some(someSequencerIndex2),
            someSequencerIndex2.sequencerTimestamp,
          ),
        )
      )
  }

  it should "correctly aggregate old and new ledger-end and synchronizer indexes" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]](
        someAggregatedLedgerEndForRepair
      )
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(someBatchOfBatches)
    aggregateLedgerEndForRepairRef.get() shouldBe
      Some(
        ParameterStorageBackend.LedgerEnd(
          lastOffset = offset(20),
          lastEventSeqId = 2020,
          lastStringInterningId = 320,
          lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
        ) -> Map(
          someSynchronizerId -> SynchronizerIndex(
            None,
            Some(someSequencerIndex2),
            someSequencerIndex2.sequencerTimestamp,
          ),
          someSynchronizerId2 -> SynchronizerIndex(
            Some(someRepairIndex2),
            Some(someSequencerIndex2),
            someSequencerIndex2.sequencerTimestamp,
          ),
        )
      )
  }

  behavior of "commitRepair"

  it should "trigger storing ledger-end on CommitRepair" in {
    val ledgerEndStoredPromise = Promise[Unit]()
    val processingEndStoredPromise = Promise[Unit]()
    val updateInMemoryStatePromise = Promise[Unit]()
    val aggregatedLedgerEnd =
      new AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]](
        Some(
          LedgerEnd(
            lastOffset = offset(1),
            lastEventSeqId = 1,
            lastStringInterningId = 1,
            lastPublicationTime = CantonTimestamp.MinValue,
          )
            -> Map.empty
        )
      )
    val input = Vector(
      offset(13) -> update,
      offset(14) -> update,
      offset(15) -> Update.CommitRepair(),
    )
    ParallelIndexerSubscription
      .commitRepair(
        storeLedgerEnd = (_, _) => {
          ledgerEndStoredPromise.success(())
          Future.unit
        },
        storePostProcessingEnd = _ => {
          processingEndStoredPromise.success(())
          Future.unit
        },
        updateInMemoryState = _ => updateInMemoryStatePromise.success(()),
        aggregatedLedgerEnd = aggregatedLedgerEnd,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(implicitly)(input)
      .futureValue shouldBe input
    ledgerEndStoredPromise.future.isCompleted shouldBe true
    processingEndStoredPromise.future.isCompleted shouldBe true
    updateInMemoryStatePromise.future.isCompleted shouldBe true
  }

  it should "not trigger storing ledger-end on non CommitRepair Updates" in {
    val ledgerEndStoredPromise = Promise[Unit]()
    val processingEndStoredPromise = Promise[Unit]()
    val updateInMemoryStatePromise = Promise[Unit]()
    val aggregatedLedgerEnd =
      new AtomicReference[Option[(LedgerEnd, Map[SynchronizerId, SynchronizerIndex])]](
        Some(
          LedgerEnd(
            lastOffset = offset(1),
            lastEventSeqId = 1,
            lastStringInterningId = 1,
            lastPublicationTime = CantonTimestamp.MinValue,
          ) -> Map.empty
        )
      )
    val input = Vector(
      offset(13) -> update,
      offset(14) -> update,
    )
    ParallelIndexerSubscription
      .commitRepair(
        storeLedgerEnd = (_, _) => {
          ledgerEndStoredPromise.success(())
          Future.unit
        },
        storePostProcessingEnd = _ => {
          processingEndStoredPromise.success(())
          Future.unit
        },
        updateInMemoryState = _ => updateInMemoryStatePromise.success(()),
        aggregatedLedgerEnd = aggregatedLedgerEnd,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(implicitly)(input)
      .futureValue shouldBe input
    ledgerEndStoredPromise.future.isCompleted shouldBe false
    processingEndStoredPromise.future.isCompleted shouldBe false
    updateInMemoryStatePromise.future.isCompleted shouldBe false
  }

  behavior of "monotonicOffsetValidator"

  it should "throw if offsets are not in a strictly increasing order" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.Epoch,
      ),
      offset(3L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      ),
      offset(2L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(2),
      ),
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe)

    testSink.request(3)
    testSink.expectNextN(offsetsUpdates.take(2))

    val error = testSink.expectError()
    error shouldBe an[AssertionError]
    error.getMessage shouldBe "assertion failed: Monotonic Offset violation detected from Offset(3) to Offset(2)"
  }

  it should "throw if offsets are not in a strictly increasing compared to the initial offset" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.Epoch,
      )
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = Some(Offset.tryFromLong(2)),
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe)

    testSink.request(1)
    val error = testSink.expectError()
    error shouldBe an[AssertionError]
    error.getMessage shouldBe "assertion failed: Monotonic Offset violation detected from Offset(2) to Offset(1)"
  }

  it should "throw if sequenced timestamps decrease" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      ),
      offset(2L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.Epoch,
      ),
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(2)
    testSink.expectNextN(offsetsUpdates.take(1))

    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex raw"assertion failed: Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(2\)"
  }

  it should "throw if sequenced timestamps decrease compared to clean synchronizer index" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      )
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ =>
            Future.successful(
              Some(
                SynchronizerIndex.of(
                  SequencerIndex(
                    sequencerTimestamp = CantonTimestamp.ofEpochSecond(10)
                  )
                )
              )
            ),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(1)
    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex raw"assertion failed: Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(1\)"
  }

  it should "throw if sequenced timestamps not increasing" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      ),
      offset(2L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      ),
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(2)
    testSink.expectNextN(offsetsUpdates.take(1))

    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex raw"assertion failed: Monotonicity violation detected: sequencer timestamp did not increase from .* to .* at offset Offset\(2\)"
  }

  it should "throw if sequenced timestamps not increasing compared to clean synchronizer index" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> Update.SequencerIndexMoved(
        synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
        recordTime = CantonTimestamp.ofEpochSecond(1),
      )
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ =>
            Future.successful(
              Some(
                SynchronizerIndex.of(
                  SequencerIndex(
                    sequencerTimestamp = CantonTimestamp.ofEpochSecond(1)
                  )
                )
              )
            ),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(1)
    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex raw"assertion failed: Monotonicity violation detected: sequencer timestamp did not increase from .* to .* at offset Offset\(1\)"
  }

  it should "throw if repair counters decrease" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> repairUpdate(CantonTimestamp.Epoch, RepairCounter(15L)),
      offset(2L) -> repairUpdate(CantonTimestamp.Epoch, RepairCounter(13L)),
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(2)
    testSink.expectNextN(offsetsUpdates.take(1))

    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex
      raw"assertion failed: Monotonicity violation detected: repair index decreases from .* to .* at offset Offset\(2\)"
  }

  it should "throw if repair counters decrease compared to clean synchronizer index" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> repairUpdate(CantonTimestamp.ofEpochSecond(10), RepairCounter(15L))
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ =>
            Future.successful(
              Some(
                SynchronizerIndex.of(
                  RepairIndex(
                    counter = RepairCounter(20L),
                    timestamp = CantonTimestamp.ofEpochSecond(10),
                  )
                )
              )
            ),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(1)
    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex
      raw"assertion failed: Monotonicity violation detected: repair index decreases from .* to .* at offset Offset\(1\)"
  }

  it should "throw if record time decreases for floating events" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> floatingUpdate(CantonTimestamp.ofEpochSecond(10)),
      offset(2L) -> floatingUpdate(CantonTimestamp.ofEpochSecond(10)),
      offset(3L) -> floatingUpdate(CantonTimestamp.Epoch),
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ => Future.successful(None),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(3)
    testSink.expectNextN(offsetsUpdates.take(2))

    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex
      raw"assertion failed: Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(3\)"
  }

  it should "throw if record time decreases for floating events compared to clean synchronizer index" in {
    val offsetsUpdates: Vector[(Offset, Update)] = Vector(
      offset(1L) -> floatingUpdate(CantonTimestamp.ofEpochSecond(10))
    )

    val testSink = Source(offsetsUpdates)
      .via(
        ParallelIndexerSubscription.monotonicityValidator(
          initialOffset = None,
          loadPreviousState = _ =>
            Future.successful(
              Some(
                SynchronizerIndex.of(
                  CantonTimestamp.ofEpochSecond(11)
                )
              )
            ),
          logger = logger,
        )
      )
      .runWith(TestSink.probe[(Offset, Update)])

    testSink.request(1)
    val error = testSink.expectError().getCause
    error shouldBe an[AssertionError]
    error.getMessage should include regex
      raw"assertion failed: Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(1\)"
  }

  def update: Update =
    Update.SequencerIndexMoved(
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      recordTime = CantonTimestamp.now(),
    )

  def repairUpdate(recordTime: CantonTimestamp, repairCounter: RepairCounter): Update =
    RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
        workflowId = None,
        preparationTime = Time.Timestamp.assertFromLong(3),
        submissionSeed = crypto.Hash.assertFromString(
          "01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086"
        ),
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      updateId = Ref.TransactionId.fromLong(15000),
      contractMetadata = Map.empty,
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      repairCounter = repairCounter,
      recordTime = recordTime,
    )(TraceContext.empty)

  def floatingUpdate(recordTime: CantonTimestamp): Update =
    TopologyTransactionEffective(
      updateId = Ref.TransactionId.fromLong(16000),
      events = Set.empty,
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      effectiveTime = recordTime,
    )(TraceContext.empty)
}
