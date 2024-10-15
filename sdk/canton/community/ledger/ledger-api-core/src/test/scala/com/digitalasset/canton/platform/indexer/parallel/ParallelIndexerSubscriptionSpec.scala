// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{
  DomainIndex,
  RequestIndex,
  SequencerIndex,
  Update,
}
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
import com.digitalasset.canton.platform.indexer.parallel.ParallelIndexerSubscription.Batch
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{DbDto, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, VersionedTransaction}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.slf4j.event.Level

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{Await, Future}

class ParallelIndexerSubscriptionSpec extends AnyFlatSpec with Matchers with NamedLogging {

  implicit val traceContext: TraceContext = TraceContext.empty
  private val serializableTraceContext =
    SerializableTraceContext(traceContext).toDamlProto.toByteArray
  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  private val someParty = DbDto.PartyEntry(
    ledger_offset = "",
    recorded_at = 0,
    submission_id = null,
    party = Some("party"),
    display_name = None,
    typ = "accept",
    rejection_reason = None,
    is_local = Some(true),
  )

  private val someDomainId: DomainId = DomainId.tryFromString("x::domainid")
  private val someDomainId2: DomainId = DomainId.tryFromString("x::domainid2")

  private val someTime = Instant.now

  private val somePartyAllocationRejected = state.Update.PartyAllocationRejected(
    submissionId = Ref.SubmissionId.assertFromString("abc"),
    participantId = Ref.ParticipantId.assertFromString("participant"),
    recordTime = Timestamp.assertFromInstant(someTime),
    rejectionReason = "reason",
  )

  private def offset(l: Long): Offset = Offset.fromLong(l)

  private val metrics = LedgerApiServerMetrics.ForTesting

  private val someEventCreated = DbDto.EventCreate(
    event_offset = "",
    update_id = "",
    ledger_effective_time = 15,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = 3,
    event_id = "",
    contract_id = "1",
    template_id = "",
    package_name = "",
    package_version = None,
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
    driver_metadata = None,
    domain_id = "x::sourcedomain",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventExercise = DbDto.EventExercise(
    consuming = true,
    event_offset = "",
    transaction_id = "",
    ledger_effective_time = 15,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = 3,
    event_id = "",
    contract_id = "1",
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    tree_event_witnesses = Set.empty,
    create_key_value = None,
    exercise_choice = "",
    exercise_argument = Array.empty,
    exercise_result = None,
    exercise_actors = Set.empty,
    exercise_child_event_ids = Vector.empty,
    create_key_value_compression = None,
    exercise_argument_compression = None,
    exercise_result_compression = None,
    event_sequential_id = 0,
    domain_id = "",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventAssign = DbDto.EventAssign(
    event_offset = "",
    update_id = "",
    command_id = None,
    workflow_id = None,
    submitter = None,
    contract_id = "",
    template_id = "",
    package_name = "",
    package_version = None,
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
    source_domain_id = "",
    target_domain_id = "",
    unassign_id = "",
    reassignment_counter = 0,
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventUnassign = DbDto.EventUnassign(
    event_offset = "",
    update_id = "",
    command_id = None,
    workflow_id = None,
    submitter = None,
    contract_id = "",
    template_id = "",
    package_name = "",
    flat_event_witnesses = Set.empty,
    event_sequential_id = 0,
    source_domain_id = "",
    target_domain_id = "",
    unassign_id = "",
    reassignment_counter = 0,
    assignment_exclusivity = None,
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someCompletion = DbDto.CommandCompletion(
    completion_offset = "",
    record_time = 0,
    publication_time = 0,
    application_id = "",
    submitters = Set.empty,
    command_id = "",
    transaction_id = None,
    rejection_status_code = None,
    rejection_status_message = None,
    rejection_status_details = None,
    submission_id = None,
    deduplication_offset = None,
    deduplication_duration_seconds = None,
    deduplication_duration_nanos = None,
    deduplication_start = None,
    domain_id = "x::sourcedomain",
    message_uuid = None,
    request_sequencer_counter = None,
    is_transaction = true,
    trace_context = serializableTraceContext,
  )

  private val offsetsAndUpdates =
    Vector(1L, 2L, 3L)
      .map(offset)
      .zip(
        Vector(
          somePartyAllocationRejected,
          somePartyAllocationRejected
            .copy(recordTime = somePartyAllocationRejected.recordTime.addMicros(1000)),
          somePartyAllocationRejected
            .copy(recordTime = somePartyAllocationRejected.recordTime.addMicros(2000)),
        ).map(Traced[Update])
      )

  behavior of "inputMapper"

  it should "provide required Batch in happy path case" in {
    val actual = ParallelIndexerSubscription.inputMapper(
      metrics = metrics,
      toDbDto = _ => _ => Iterator(someParty, someParty),
      toMeteringDbDto = _ => Vector.empty,
      logger,
    )(
      List(
        Offset.fromLong(1),
        Offset.fromLong(2),
        Offset.fromLong(3),
      ).zip(offsetsAndUpdates.map(_._2))
    )
    val expected = Batch[Vector[DbDto]](
      lastOffset = offset(3),
      lastSeqEventId = 0,
      lastStringInterningId = 0,
      lastRecordTime = someTime.plusMillis(2).toEpochMilli,
      publicationTime = CantonTimestamp.MinValue,
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

  behavior of "inputMapper transaction metering"

  it should "extract transaction metering" in {

    val applicationId = Ref.ApplicationId.assertFromString("a0")

    val timestamp: Long = 12345
    val offset = Ref.HexString.assertFromString("02")
    val someHash = Hash.hashPrivateKey("p0")

    val someRecordTime =
      Time.Timestamp.assertFromInstant(Instant.parse("2000-01-01T00:00:00.000000Z"))

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
      messageUuid = None,
    )
    val someTransactionMeta = state.TransactionMeta(
      ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
      workflowId = None,
      submissionTime = Time.Timestamp.assertFromLong(3),
      submissionSeed = someHash,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

    val someTransactionAccepted = state.Update.TransactionAccepted(
      completionInfoO = Some(someCompletionInfo),
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(
        VersionedTransaction(LanguageVersion.v2_dev, Map.empty, ImmArray.empty)
      ),
      updateId = Ref.TransactionId.assertFromString("UpdateId"),
      recordTime = someRecordTime,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = DomainId.tryFromString("da::default"),
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.now())
        )
      ),
    )

    val expected: Vector[DbDto.TransactionMetering] = Vector(
      DbDto.TransactionMetering(
        application_id = applicationId,
        action_count = 0,
        metering_timestamp = timestamp,
        ledger_offset = offset,
      )
    )

    val actual: Vector[DbDto.TransactionMetering] = ParallelIndexerSubscription
      .inputMapper(
        metrics = metrics,
        toDbDto = _ => _ => Iterator.empty,
        toMeteringDbDto = _ => expected,
        logger,
      )(
        List(
          (Offset.fromHexString(offset), Traced[Update](someTransactionAccepted))
        )
      )
      .batch
      .asInstanceOf[Vector[DbDto.TransactionMetering]]

    actual shouldBe expected

  }

  behavior of "seqMapperZero"

  it should "provide required Batch in happy path case" in {
    val initialPublicationTime = CantonTimestamp.now()
    ParallelIndexerSubscription.seqMapperZero(123, 234, initialPublicationTime) shouldBe Batch(
      lastOffset = Offset.beforeBegin,
      lastSeqEventId = 123,
      lastStringInterningId = 234,
      lastRecordTime = 0,
      publicationTime = initialPublicationTime,
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
    val result = ParallelIndexerSubscription.seqMapper(
      internize = _.zipWithIndex.map(x => x._2 -> x._2.toString).take(2),
      metrics,
      simClock,
      logger,
    )(
      previous = ParallelIndexerSubscription.seqMapperZero(15, 26, previousPublicationTime),
      current = Batch(
        lastOffset = offset(2),
        lastSeqEventId = 0,
        lastStringInterningId = 0,
        lastRecordTime = someTime.toEpochMilli,
        publicationTime = CantonTimestamp.MinValue,
        lastTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someEventCreated,
          DbDto.IdFilterCreateStakeholder(0L, "", ""),
          DbDto.IdFilterCreateNonStakeholderInformee(0L, ""),
          DbDto.IdFilterConsumingStakeholder(0L, "", ""),
          DbDto.IdFilterConsumingNonStakeholderInformee(0L, ""),
          DbDto.IdFilterNonConsumingInformee(0L, ""),
          someEventCreated,
          someEventCreated,
          DbDto.TransactionMeta("", "", 0L, 0L, "x::sourcedomain", 0L, 0L),
          someParty,
          someEventExercise,
          DbDto.TransactionMeta("", "", 0L, 0L, "x::sourcedomain", 0L, 0L),
          someParty,
          someEventAssign,
          DbDto.IdFilterAssignStakeholder(0L, "", ""),
          DbDto.IdFilterAssignStakeholder(0L, "", ""),
          DbDto.TransactionMeta("", "", 0L, 0L, "x::sourcedomain", 0L, 0L),
          someParty,
          someEventUnassign,
          DbDto.IdFilterUnassignStakeholder(0L, "", ""),
          DbDto.IdFilterUnassignStakeholder(0L, "", ""),
          DbDto.TransactionMeta("", "", 0L, 0L, "x::sourcedomain", 0L, 0L),
          someParty,
          someCompletion,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
      ),
    )
    import scala.util.chaining.*

    result.lastSeqEventId shouldBe 21
    result.lastStringInterningId shouldBe 1
    result.publicationTime shouldBe currentPublicationTime
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
    val simClock = new SimClock(loggerFactory = loggerFactory)
    val result = ParallelIndexerSubscription.seqMapper(_ => Nil, metrics, simClock, logger)(
      ParallelIndexerSubscription.seqMapperZero(15, 25, CantonTimestamp.now()),
      Batch(
        lastOffset = offset(2),
        lastSeqEventId = 0,
        lastStringInterningId = 0,
        lastRecordTime = someTime.toEpochMilli,
        publicationTime = CantonTimestamp.MinValue,
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
    result.lastSeqEventId shouldBe 15
    result.lastStringInterningId shouldBe 25
  }

  it should "throw if offset going backwards in the new batch" in {
    val simClock = new SimClock(loggerFactory = loggerFactory)
    intercept[AssertionError](
      ParallelIndexerSubscription.seqMapper(_ => Nil, metrics, simClock, logger)(
        ParallelIndexerSubscription.seqMapperZero(15, 25, CantonTimestamp.now()),
        Batch(
          lastOffset = offset(2),
          lastSeqEventId = 0,
          lastStringInterningId = 0,
          lastRecordTime = someTime.toEpochMilli,
          publicationTime = CantonTimestamp.MinValue,
          lastTraceContext = TraceContext.empty,
          batch = Vector(
            someParty,
            someParty,
            someParty,
            someParty,
          ),
          batchSize = 3,
          offsetsUpdates = Vector(1L, 3L, 2L)
            .map(offset)
            .zip(
              Vector(
                somePartyAllocationRejected,
                somePartyAllocationRejected
                  .copy(recordTime = somePartyAllocationRejected.recordTime.addMicros(1000)),
                somePartyAllocationRejected
                  .copy(recordTime = somePartyAllocationRejected.recordTime.addMicros(2000)),
              ).map(Traced[Update])
            ),
        ),
      )
    ).getMessage shouldBe "assertion failed: Monotonic Offset violation detected from Offset(Bytes(000000000000000003)) to Offset(Bytes(000000000000000002))"
  }

  it should "throw if offset going backwards cross batch" in {
    val simClock = new SimClock(loggerFactory = loggerFactory)
    intercept[AssertionError](
      ParallelIndexerSubscription.seqMapper(_ => Nil, metrics, simClock, logger)(
        ParallelIndexerSubscription
          .seqMapperZero(15, 25, CantonTimestamp.now())
          .copy(lastOffset = offset(2)),
        Batch(
          lastOffset = offset(2),
          lastSeqEventId = 0,
          lastStringInterningId = 0,
          lastRecordTime = someTime.toEpochMilli,
          publicationTime = CantonTimestamp.MinValue,
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
    ).getMessage shouldBe "assertion failed: Monotonic Offset violation detected from Offset(Bytes(000000000000000002)) to Offset(Bytes(000000000000000001))"
  }

  it should "take the last publication time, if bigger than the current time, and log" in {
    val now = CantonTimestamp.now()
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val previous = now.plusSeconds(10)
    loggerFactory.assertLogs(
      LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(Level.INFO)
    )(
      ParallelIndexerSubscription
        .seqMapper(_ => Nil, metrics, simClock, logger)(
          ParallelIndexerSubscription.seqMapperZero(15, 25, previous),
          Batch(
            lastOffset = offset(2),
            lastSeqEventId = 0,
            lastStringInterningId = 0,
            lastRecordTime = someTime.toEpochMilli,
            publicationTime = CantonTimestamp.MinValue,
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
        .publicationTime shouldBe previous,
      _.infoMessage should include("Has the clock been reset, e.g., during participant failover?"),
    )
  }

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerSubscription.batcher(
      batchF = _ => "bumm"
    )(
      Batch(
        lastOffset = offset(2),
        lastSeqEventId = 0,
        lastRecordTime = someTime.toEpochMilli,
        publicationTime = CantonTimestamp.MinValue,
        lastStringInterningId = 0,
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
      lastOffset = offset(2),
      lastSeqEventId = 0,
      lastStringInterningId = 0,
      lastRecordTime = someTime.toEpochMilli,
      publicationTime = CantonTimestamp.MinValue,
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

      override val executor = new QueueAwareExecutor with NamedExecutor {
        override def queueSize: Long = 0
        override def name: String = "test"
      }

    }

    val batchPayload = "Some batch payload"

    val ingestFunction: (Connection, String) => Unit = {
      case (`connection`, `batchPayload`) => ()
      case other => fail(s"Unexpected: $other")
    }

    val inBatch = Batch(
      lastOffset = offset(2),
      lastSeqEventId = 2000,
      lastStringInterningId = 300,
      lastRecordTime = someTime.toEpochMilli,
      publicationTime = CantonTimestamp.MinValue,
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
          override def persist(updates: Seq[(Update, Offset)], tracedLogger: TracedLogger)(implicit
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
        lastOffset = offset(2),
        lastSeqEventId = 2000,
        lastStringInterningId = 300,
        lastRecordTime = someTime.toEpochMilli,
        publicationTime = CantonTimestamp.MinValue,
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

    val storeLedgerEndF: (LedgerEnd, Map[DomainId, DomainIndex]) => Future[Unit] = {
      case (`secondBatchLedgerEnd`, _) => Future.unit
      case otherLedgerEnd => fail(s"Unexpected ledger end: $otherLedgerEnd")
    }

    val batch = Batch(
      lastOffset = ledgerEnd.lastOffset,
      lastSeqEventId = ledgerEnd.lastEventSeqId,
      lastStringInterningId = ledgerEnd.lastStringInterningId,
      lastRecordTime = someTime.toEpochMilli,
      publicationTime = ledgerEnd.lastPublicationTime,
      lastTraceContext = TraceContext.empty,
      batch = "Some batch payload",
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

    val batchOfBatches = Vector(
      batch,
      batch.copy(
        lastSeqEventId = secondBatchLedgerEnd.lastEventSeqId,
        lastOffset = secondBatchLedgerEnd.lastOffset,
        lastStringInterningId = secondBatchLedgerEnd.lastStringInterningId,
        publicationTime = secondBatchLedgerEnd.lastPublicationTime,
      ),
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

  behavior of "ledgerEndFromBatch"

  it should "populate correct ledger-end from batch in happy path case" in {
    ParallelIndexerSubscription.ledgerEndFrom(
      Batch(
        lastOffset = offset(5),
        lastSeqEventId = 2000,
        lastStringInterningId = 300,
        lastRecordTime = someTime.toEpochMilli,
        publicationTime = CantonTimestamp.MinValue.plusSeconds(20),
        lastTraceContext = TraceContext.empty,
        batch = "zero",
        batchSize = 0,
        offsetsUpdates = Vector.empty,
      )
    ) shouldBe ParameterStorageBackend.LedgerEnd(
      lastOffset = offset(5),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
      lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(20),
    )
  }

  behavior of "domainLedgerEndFromBatch"

  private val someSequencerIndex1 = SequencerIndex(
    counter = SequencerCounter(11),
    timestamp = CantonTimestamp.ofEpochMicro(123),
  )
  private val someSequencerIndex2 = SequencerIndex(
    counter = SequencerCounter(20),
    timestamp = CantonTimestamp.ofEpochMicro(256),
  )
  private val someRequestIndex1 = RequestIndex(
    counter = RequestCounter(5),
    sequencerCounter = Some(SequencerCounter(15)),
    timestamp = CantonTimestamp.ofEpochMicro(153),
  )
  private val someRequestIndex2 = RequestIndex(
    counter = RequestCounter(6),
    sequencerCounter = None,
    timestamp = CantonTimestamp.ofEpochMicro(156),
  )

  it should "populate correct ledger-end from batches for a sequencer counter moved" in {
    ParallelIndexerSubscription.ledgerEndDomainIndexFrom(
      Vector(someDomainId -> DomainIndex.of(someSequencerIndex1))
    ) shouldBe Map(
      someDomainId -> DomainIndex.of(someSequencerIndex1)
    )
  }

  it should "populate correct ledger-end from batches for a request counter moved" in {
    ParallelIndexerSubscription.ledgerEndDomainIndexFrom(
      Vector(someDomainId -> DomainIndex.of(someRequestIndex1))
    ) shouldBe Map(
      someDomainId -> DomainIndex.of(someRequestIndex1)
    )
  }

  it should "populate correct ledger-end from batches for a mixed batch" in {
    ParallelIndexerSubscription.ledgerEndDomainIndexFrom(
      Vector(
        someDomainId -> DomainIndex.of(someSequencerIndex1),
        someDomainId -> DomainIndex.of(someSequencerIndex2),
        someDomainId2 -> DomainIndex.of(someRequestIndex1),
        someDomainId2 -> DomainIndex.of(someRequestIndex2),
      )
    ) shouldBe Map(
      someDomainId -> DomainIndex(
        None,
        Some(someSequencerIndex2),
      ),
      someDomainId2 -> DomainIndex(
        Some(someRequestIndex2),
        Some(
          SequencerIndex(
            counter = SequencerCounter(15),
            timestamp = CantonTimestamp.ofEpochMicro(153),
          )
        ),
      ),
    )
  }

  it should "populate correct ledger-end from batches for a mixed batch 2" in {
    ParallelIndexerSubscription.ledgerEndDomainIndexFrom(
      Vector(
        someDomainId -> DomainIndex.of(someSequencerIndex1),
        someDomainId -> DomainIndex.of(someRequestIndex1),
        someDomainId2 -> DomainIndex.of(someSequencerIndex1),
        someDomainId2 -> DomainIndex.of(someRequestIndex2),
      )
    ) shouldBe Map(
      someDomainId -> DomainIndex(
        Some(someRequestIndex1),
        Some(
          SequencerIndex(
            counter = SequencerCounter(15),
            timestamp = CantonTimestamp.ofEpochMicro(153),
          )
        ),
      ),
      someDomainId2 -> DomainIndex(
        Some(someRequestIndex2),
        Some(someSequencerIndex1),
      ),
    )
  }

  behavior of "aggregateLedgerEndForRepair"

  private val someAggregatedLedgerEndForRepair: (LedgerEnd, Map[DomainId, DomainIndex]) =
    ParameterStorageBackend.LedgerEnd(
      lastOffset = offset(5),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
      lastPublicationTime = CantonTimestamp.ofEpochMicro(5),
    ) -> Map(
      someDomainId -> DomainIndex(
        None,
        Some(
          SequencerIndex(
            counter = SequencerCounter(5),
            timestamp = CantonTimestamp.ofEpochMicro(5),
          )
        ),
      ),
      someDomainId2 -> DomainIndex(
        Some(someRequestIndex2),
        Some(
          SequencerIndex(
            counter = SequencerCounter(4),
            timestamp = CantonTimestamp.ofEpochMicro(4),
          )
        ),
      ),
    )

  private val someBatchOfBatches: Vector[Batch[Unit]] = Vector(
    Batch(
      lastOffset = offset(10),
      lastSeqEventId = 2010,
      lastStringInterningId = 310,
      lastRecordTime = someTime.toEpochMilli + 10,
      publicationTime = CantonTimestamp.ofEpochMicro(15),
      lastTraceContext = TraceContext.empty,
      batch = (),
      batchSize = 0,
      offsetsUpdates = Vector(
        offset(9) -> Traced(
          Update.SequencerIndexMoved(
            domainId = someDomainId,
            sequencerIndex = someSequencerIndex1,
            requestCounterO = None,
          )
        ),
        offset(10) -> Traced(
          Update.SequencerIndexMoved(
            domainId = someDomainId2,
            sequencerIndex = someSequencerIndex1,
            requestCounterO = None,
          )
        ),
      ),
    ),
    Batch(
      lastOffset = offset(20),
      lastSeqEventId = 2020,
      lastStringInterningId = 320,
      lastRecordTime = someTime.toEpochMilli + 20,
      publicationTime = CantonTimestamp.ofEpochMicro(25),
      lastTraceContext = TraceContext.empty,
      batch = (),
      batchSize = 0,
      offsetsUpdates = Vector(
        offset(19) -> Traced(
          Update.SequencerIndexMoved(
            domainId = someDomainId,
            sequencerIndex = someSequencerIndex2,
            requestCounterO = None,
          )
        ),
        offset(20) -> Traced(
          Update.SequencerIndexMoved(
            domainId = someDomainId2,
            sequencerIndex = someSequencerIndex2,
            requestCounterO = None,
          )
        ),
      ),
    ),
  )

  it should "correctly aggregate if batch has no new domain-indexes" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[(LedgerEnd, Map[DomainId, DomainIndex])](someAggregatedLedgerEndForRepair)
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(Vector.empty)
    aggregateLedgerEndForRepairRef.get() shouldBe someAggregatedLedgerEndForRepair
  }

  it should "correctly aggregate if old state is empty" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[(LedgerEnd, Map[DomainId, DomainIndex])](
        ParallelIndexerSubscription.DefaultAggregatedLedgerEndForRepair
      )
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(someBatchOfBatches)
    aggregateLedgerEndForRepairRef.get() shouldBe
      ParameterStorageBackend.LedgerEnd(
        lastOffset = offset(20),
        lastEventSeqId = 2020,
        lastStringInterningId = 320,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
      ) -> Map(
        someDomainId -> DomainIndex(
          None,
          Some(someSequencerIndex2),
        ),
        someDomainId2 -> DomainIndex(
          None,
          Some(someSequencerIndex2),
        ),
      )
  }

  it should "correctly aggregate old and new ledger-end and domain indexes" in {
    val aggregateLedgerEndForRepairRef =
      new AtomicReference[(LedgerEnd, Map[DomainId, DomainIndex])](
        someAggregatedLedgerEndForRepair
      )
    ParallelIndexerSubscription
      .aggregateLedgerEndForRepair(aggregateLedgerEndForRepairRef)
      .apply(someBatchOfBatches)
    aggregateLedgerEndForRepairRef.get() shouldBe
      ParameterStorageBackend.LedgerEnd(
        lastOffset = offset(20),
        lastEventSeqId = 2020,
        lastStringInterningId = 320,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
      ) -> Map(
        someDomainId -> DomainIndex(
          None,
          Some(someSequencerIndex2),
        ),
        someDomainId2 -> DomainIndex(
          Some(someRequestIndex2),
          Some(someSequencerIndex2),
        ),
      )
  }

}
