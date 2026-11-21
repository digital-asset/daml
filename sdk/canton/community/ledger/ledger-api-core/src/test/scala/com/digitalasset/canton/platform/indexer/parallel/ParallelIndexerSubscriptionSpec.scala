// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageIds
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
  ActivationRef,
  Batch,
  EmptyActiveContracts,
  SynCon,
  ZeroLedgerEnd,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{
  DbDto,
  ParameterStorageBackend,
  ScalatestEqualityHelpers,
}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
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
import scala.annotation.unused
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class ParallelIndexerSubscriptionSpec
    extends AnyFlatSpec
    with ScalaFutures
    with Matchers
    with NamedLogging {

  implicit private val DbDtoEqual: org.scalactic.Equality[DbDto] = ScalatestEqualityHelpers.DbDtoEq
  implicit val traceContext: TraceContext = TraceContext.empty
  private val serializableTraceContext =
    SerializableTraceContext(traceContext).toDamlProto.toByteArray
  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  implicit val actorSystem: ActorSystem = ActorSystem(
    classOf[ParallelIndexerSubscriptionSpec].getSimpleName
  )
  implicit val materializer: Materializer = Materializer(actorSystem)
  val emptyByteArray = new Array[Byte](0)

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

  private val updateId = TestUpdateId("mock_hash")
  private val updateIdByteArray = updateId.toProtoPrimitive.toByteArray

  private def offset(l: Long): Offset = Offset.tryFromLong(l)

  private val metrics = LedgerApiServerMetrics.ForTesting

  private def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  private val someEventActivate = DbDto.EventActivate(
    event_offset = 1,
    update_id = updateIdByteArray,
    workflow_id = None,
    command_id = None,
    submitters = None,
    record_time = 1,
    synchronizer_id = someSynchronizerId,
    trace_context = serializableTraceContext,
    external_transaction_hash = None,
    event_type = 1,
    event_sequential_id = 15,
    node_id = 3,
    additional_witnesses = None,
    source_synchronizer_id = None,
    reassignment_counter = None,
    reassignment_id = None,
    representative_package_id = "",
    notPersistedContractId = hashCid("1"),
    internal_contract_id = 1,
    create_key_hash = None,
  )

  private val someEventDeactivate = DbDto.EventDeactivate(
    event_offset = 1,
    update_id = updateIdByteArray,
    workflow_id = None,
    command_id = None,
    submitters = None,
    record_time = 1,
    synchronizer_id = someSynchronizerId,
    trace_context = serializableTraceContext,
    external_transaction_hash = None,
    event_type = 1,
    event_sequential_id = 1,
    node_id = 1,
    deactivated_event_sequential_id = None,
    additional_witnesses = None,
    exercise_choice = None,
    exercise_choice_interface_id = None,
    exercise_argument = None,
    exercise_result = None,
    exercise_actors = None,
    exercise_last_descendant_node_id = None,
    exercise_argument_compression = None,
    exercise_result_compression = None,
    reassignment_id = None,
    assignment_exclusivity = None,
    target_synchronizer_id = None,
    reassignment_counter = None,
    contract_id = hashCid("1"),
    internal_contract_id = None,
    template_id = "",
    package_id = "",
    stakeholders = Set.empty,
    ledger_effective_time = None,
  )

  private val someEventWitnessed = DbDto.EventVariousWitnessed(
    event_offset = 1,
    update_id = updateIdByteArray,
    workflow_id = None,
    command_id = None,
    submitters = None,
    record_time = 1,
    synchronizer_id = someSynchronizerId,
    trace_context = serializableTraceContext,
    external_transaction_hash = None,
    event_type = 1,
    event_sequential_id = 1,
    node_id = 1,
    additional_witnesses = Set.empty,
    consuming = None,
    exercise_choice = None,
    exercise_choice_interface_id = None,
    exercise_argument = None,
    exercise_result = None,
    exercise_actors = None,
    exercise_last_descendant_node_id = None,
    exercise_argument_compression = None,
    exercise_result_compression = None,
    representative_package_id = None,
    contract_id = None,
    internal_contract_id = None,
    template_id = None,
    package_id = None,
    ledger_effective_time = None,
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
    synchronizer_id = someSynchronizerId,
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

  private def mockDbDispatcher(connection: Connection): DbDispatcher = new DbDispatcher {
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
      batchTraceContext = TraceContext.empty,
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
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
    )
    actual.copy(batchTraceContext = TraceContext.empty) shouldBe expected
    actual.activeContracts eq ParallelIndexerSubscription.EmptyActiveContracts
  }

  behavior of "seqMapperZero"

  it should "provide required Batch in happy path case" in {
    val ledgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 123,
      lastStringInterningId = 234,
      lastPublicationTime = CantonTimestamp.now(),
    )

    val result = ParallelIndexerSubscription.seqMapperZero(Some(ledgerEnd))
    result shouldBe Batch(
      ledgerEnd = ledgerEnd,
      batchTraceContext = TraceContext.empty,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
    )
    result.activeContracts eq ParallelIndexerSubscription.EmptyActiveContracts
  }

  it should "provide required Batch in case starting from scratch" in {
    ParallelIndexerSubscription.seqMapperZero(None) shouldBe Batch(
      ledgerEnd = ZeroLedgerEnd,
      batchTraceContext = TraceContext.empty,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
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
    val ledgerEndCache = MutableLedgerEndCache()
    val result = ParallelIndexerSubscription.seqMapper(
      internize = _.zipWithIndex.map(x => x._2 -> x._2.toString).take(2),
      metrics,
      simClock,
      logger,
      ledgerEndCache,
    )(
      previous = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
      current = Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        batchTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someEventActivate,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).activateStakeholder,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).activateWitness,
          DbDto.TransactionMeta(emptyByteArray, 1, 0L, 0L, someSynchronizerId, 0L, 0L),
          someCompletion,
          someParty,
          someEventDeactivate,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).deactivateStakeholder,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).deactivateWitness,
          someEventDeactivate,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).deactivateStakeholder,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).deactivateWitness,
          DbDto.TransactionMeta(emptyByteArray, 1, 0L, 0L, someSynchronizerId, 0L, 0L),
          someParty,
          someEventWitnessed,
          DbDto.IdFilter(0, "", "", first_per_sequential_id = false).variousWitness,
          DbDto.TransactionMeta(emptyByteArray, 1, 0L, 0L, someSynchronizerId, 0L, 0L),
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
        missingDeactivatedActivations = Map.empty,
        activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
      ),
    )
    import scala.util.chaining.*

    result.ledgerEnd.lastEventSeqId shouldBe 19
    result.ledgerEnd.lastStringInterningId shouldBe 1
    result.ledgerEnd.lastPublicationTime shouldBe currentPublicationTime
    result.ledgerEnd.lastOffset shouldBe offset(2)
    result.batch(1).asInstanceOf[DbDto.EventActivate].event_sequential_id shouldBe 16
    result
      .batch(2)
      .asInstanceOf[DbDto.IdFilterActivateStakeholder]
      .idFilter
      .event_sequential_id shouldBe 16
    result
      .batch(3)
      .asInstanceOf[DbDto.IdFilterActivateWitness]
      .idFilter
      .event_sequential_id shouldBe 16
    result.batch(4).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 16L
      transactionMeta.event_sequential_id_last shouldBe 16L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result
      .batch(5)
      .asInstanceOf[DbDto.CommandCompletion]
      .publication_time shouldBe currentPublicationTime.toMicros
    result.batch(7).asInstanceOf[DbDto.EventDeactivate].event_sequential_id shouldBe 17
    result
      .batch(8)
      .asInstanceOf[DbDto.IdFilterDeactivateStakeholder]
      .idFilter
      .event_sequential_id shouldBe 17
    result
      .batch(9)
      .asInstanceOf[DbDto.IdFilterDeactivateWitness]
      .idFilter
      .event_sequential_id shouldBe 17
    result.batch(10).asInstanceOf[DbDto.EventDeactivate].event_sequential_id shouldBe 18
    result
      .batch(11)
      .asInstanceOf[DbDto.IdFilterDeactivateStakeholder]
      .idFilter
      .event_sequential_id shouldBe 18
    result
      .batch(12)
      .asInstanceOf[DbDto.IdFilterDeactivateWitness]
      .idFilter
      .event_sequential_id shouldBe 18
    result.batch(13).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 17L
      transactionMeta.event_sequential_id_last shouldBe 18L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result.batch(15).asInstanceOf[DbDto.EventVariousWitnessed].event_sequential_id shouldBe 19
    result
      .batch(16)
      .asInstanceOf[DbDto.IdFilterVariousWitness]
      .idFilter
      .event_sequential_id shouldBe 19
    result.batch(17).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 19L
      transactionMeta.event_sequential_id_last shouldBe 19L
      transactionMeta.publication_time shouldBe currentPublicationTime.toMicros
    }
    result.batch(19).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 0
    result.batch(19).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "0"
    result.batch(20).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 1
    result.batch(20).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "1"
  }

  it should "preserve sequence id if nothing to assign" in {
    val previousLedgerEnd = LedgerEnd(
      lastOffset = offset(1),
      lastEventSeqId = 15,
      lastStringInterningId = 25,
      lastPublicationTime = CantonTimestamp.now(),
    )
    val simClock = new SimClock(loggerFactory = loggerFactory)
    val result = ParallelIndexerSubscription.seqMapper(
      _ => Nil,
      metrics,
      simClock,
      logger,
      MutableLedgerEndCache(),
    )(
      ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
      Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        batchTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someParty,
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
        missingDeactivatedActivations = Map.empty,
        activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
      ),
    )
    result.ledgerEnd.lastEventSeqId shouldBe 15
    result.ledgerEnd.lastStringInterningId shouldBe 25
    result.ledgerEnd.lastOffset shouldBe offset(2)
  }

  private val now = CantonTimestamp.now()
  private val previous = now.plusSeconds(10)
  private val previousLedgerEnd = LedgerEnd(
    lastOffset = offset(1),
    lastEventSeqId = 15,
    lastStringInterningId = 25,
    lastPublicationTime = previous,
  )
  private val simClock = new SimClock(now, loggerFactory = loggerFactory)

  it should "take the last publication time, if bigger than the current time, and log" in {
    loggerFactory.assertLogs(
      LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(Level.INFO)
    )(
      ParallelIndexerSubscription
        .seqMapper(_ => Nil, metrics, simClock, logger, MutableLedgerEndCache())(
          ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd)),
          Batch(
            ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
            batchTraceContext = TraceContext.empty,
            batch = Vector(
              someParty,
              someParty,
              someParty,
              someParty,
            ),
            batchSize = 3,
            offsetsUpdates = offsetsAndUpdates,
            missingDeactivatedActivations = Map.empty,
            activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
          ),
        )
        .ledgerEnd
        .lastPublicationTime shouldBe previous,
      _.infoMessage should include("Has the clock been reset, e.g., during participant failover?"),
    )
  }

  it should "activations are added to the ACS" in {
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val zeroBatch = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd))
    val ledgerEndCache = MutableLedgerEndCache()
    val result = ParallelIndexerSubscription
      .seqMapper(_ => Nil, metrics, simClock, logger, ledgerEndCache)(
        zeroBatch,
        Batch(
          ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
          batchTraceContext = TraceContext.empty,
          batch = Vector(
            someEventActivate.copy(
              synchronizer_id = someSynchronizerId2,
              notPersistedContractId = hashCid("C"),
            )
          ),
          batchSize = 10,
          offsetsUpdates = offsetsAndUpdates,
          missingDeactivatedActivations = Map.empty,
          activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
        ),
      )
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId2, hashCid("C")) -> ActivationRef(16L, 1L)
    )
    result.missingDeactivatedActivations shouldBe Map.empty
  }

  it should "double activations are reported as warnings" in {
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val zeroBatch = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd))
    val ledgerEndCache = MutableLedgerEndCache()
    zeroBatch.activeContracts.addAll(
      Seq(
        SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(1L, 1L),
        SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(2L, 2L),
      )
    )
    val result = loggerFactory.assertLogs(
      LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(Level.WARN)
    )(
      ParallelIndexerSubscription
        .seqMapper(_ => Nil, metrics, simClock, logger, ledgerEndCache)(
          zeroBatch,
          Batch(
            ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
            batchTraceContext = TraceContext.empty,
            batch = Vector(
              someEventActivate.copy(
                synchronizer_id = someSynchronizerId,
                notPersistedContractId = hashCid("A"),
                internal_contract_id = 10L,
              ),
              someEventActivate.copy(
                synchronizer_id = someSynchronizerId2,
                notPersistedContractId = hashCid("B"),
                internal_contract_id = 20L,
              ),
            ),
            batchSize = 10,
            offsetsUpdates = offsetsAndUpdates,
            missingDeactivatedActivations = Map.empty,
            activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
          ),
        ),
      _.warningMessage should include(
        "Double activation at eventSeqId: 16. Previous at Some(1) This should not happen"
      ),
      _.warningMessage should include(
        "Double activation at eventSeqId: 17. Previous at Some(2) This should not happen"
      ),
    )
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(16L, 10L),
      SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(17L, 20L),
    )
    result.missingDeactivatedActivations shouldBe Map.empty
  }

  it should "deactivation is extending the missing activations if not found (but not for divulged or non-consumed contracts)" in {
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val zeroBatch = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd))
    val ledgerEndCache = MutableLedgerEndCache()
    val result = ParallelIndexerSubscription
      .seqMapper(_ => Nil, metrics, simClock, logger, ledgerEndCache)(
        zeroBatch,
        Batch(
          ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
          batchTraceContext = TraceContext.empty,
          batch = Vector(
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("A"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("E"),
            ),
            someEventWitnessed.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = Some(hashCid("C")),
            ),
            someEventWitnessed.copy(
              consuming = Some(false),
              synchronizer_id = someSynchronizerId,
              contract_id = Some(hashCid("D")),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("B"),
            ),
          ),
          batchSize = 10,
          offsetsUpdates = offsetsAndUpdates,
          missingDeactivatedActivations = Map.empty,
          activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
        ),
      )
    zeroBatch.activeContracts shouldBe Map.empty
    result.missingDeactivatedActivations shouldBe Map(
      SynCon(someSynchronizerId, hashCid("A")) -> None,
      SynCon(someSynchronizerId, hashCid("B")) -> None,
      SynCon(someSynchronizerId2, hashCid("E")) -> None,
    )
    result.batch
      .collect { case u: DbDto.EventDeactivate =>
        u.deactivated_event_sequential_id
      }
      .shouldBe(
        Seq(
          None,
          None,
          None,
        )
      )
  }

  it should "deactivation is computed directly from the active contracts if it has it - and also removing activeness thereof" in {
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val zeroBatch = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd))
    val ledgerEndCache = MutableLedgerEndCache()
    zeroBatch.activeContracts
      .addAll(
        Seq(
          SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(1L, 100L),
          SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(2L, 200L),
          SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(3L, 300L),
          SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(4L, 400L),
          SynCon(someSynchronizerId3, hashCid("C")) -> ActivationRef(5L, 500L),
          SynCon(someSynchronizerId, hashCid("C")) -> ActivationRef(6L, 600L),
        )
      )
    val result = ParallelIndexerSubscription
      .seqMapper(_ => Nil, metrics, simClock, logger, ledgerEndCache)(
        zeroBatch,
        Batch(
          ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
          batchTraceContext = TraceContext.empty,
          batch = Vector(
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId3,
              contract_id = hashCid("C"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("A"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("B"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("C"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("A"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("B"),
            ),
          ),
          batchSize = 10,
          offsetsUpdates = offsetsAndUpdates,
          missingDeactivatedActivations = Map.empty,
          activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
        ),
      )
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(3L, 300L),
      SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(4L, 400L),
      SynCon(someSynchronizerId, hashCid("C")) -> ActivationRef(6L, 600L),
    )
    result.missingDeactivatedActivations shouldBe Map(
      SynCon(someSynchronizerId2, hashCid("A")) -> None,
      SynCon(someSynchronizerId, hashCid("B")) -> None,
      SynCon(someSynchronizerId2, hashCid("C")) -> None,
    )
    result.batch
      .collect { case u: DbDto.EventDeactivate =>
        u.deactivated_event_sequential_id -> u.internal_contract_id
      }
      .shouldBe(
        Seq(
          Some(5L) -> Some(500L),
          None -> None,
          None -> None,
          None -> None,
          Some(1L) -> Some(100L),
          Some(2L) -> Some(200L),
        )
      )
  }

  it should "activations pruned correctly based on actual ledger-end" in {
    val simClock = new SimClock(now, loggerFactory = loggerFactory)
    val zeroBatch = ParallelIndexerSubscription.seqMapperZero(Some(previousLedgerEnd))
    val ledgerEndCache = MutableLedgerEndCache()
    zeroBatch.activeContracts
      .addAll(
        Seq(
          SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(100L, 1L),
          SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(110L, 2L),
          SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(120L, 3L),
          SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(130L, 4L),
        )
      )
    def processSeqMapper() = ParallelIndexerSubscription
      .seqMapper(_ => Nil, metrics, simClock, logger, ledgerEndCache)(
        zeroBatch,
        Batch(
          ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
          batchTraceContext = TraceContext.empty,
          batch = Vector(
            someParty
          ),
          batchSize = 10,
          offsetsUpdates = offsetsAndUpdates,
          missingDeactivatedActivations = Map.empty,
          activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
        ),
      )
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(100L, 1L),
      SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(110L, 2L),
      SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(120L, 3L),
      SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(130L, 4L),
    )

    // ledger end below
    ledgerEndCache.set(
      Some(
        previousLedgerEnd.copy(
          lastEventSeqId = 10
        )
      )
    )
    processSeqMapper()
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId, hashCid("A")) -> ActivationRef(100L, 1L),
      SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(110L, 2L),
      SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(120L, 3L),
      SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(130L, 4L),
    )

    // ledger end on first
    ledgerEndCache.set(
      Some(
        previousLedgerEnd.copy(
          lastEventSeqId = 100L
        )
      )
    )
    processSeqMapper()
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId2, hashCid("B")) -> ActivationRef(110L, 2L),
      SynCon(someSynchronizerId3, hashCid("A")) -> ActivationRef(120L, 3L),
      SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(130L, 4L),
    )

    // ledger end after third
    ledgerEndCache.set(
      Some(
        previousLedgerEnd.copy(
          lastEventSeqId = 125L
        )
      )
    )
    processSeqMapper()
    zeroBatch.activeContracts shouldBe Map(
      SynCon(someSynchronizerId3, hashCid("B")) -> ActivationRef(130L, 4L)
    )
  }

  behavior of "refillMissingDeactivatedActivations"

  it should "correctly refill the missing activations" in {
    ParallelIndexerSubscription
      .refillMissingDeactivatedActivations(logger)(
        Batch(
          ledgerEnd = previousLedgerEnd,
          batch = Vector(
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("A"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("C"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("B"),
            ),
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId,
              contract_id = hashCid("B"),
              deactivated_event_sequential_id = Some(10000),
              internal_contract_id = Some(100000),
            ),
          ),
          batchSize = 1,
          offsetsUpdates = Vector.empty,
          activeContracts = EmptyActiveContracts,
          missingDeactivatedActivations = Map(
            SynCon(someSynchronizerId2, hashCid("A")) -> Some(ActivationRef(123, 1230)),
            SynCon(someSynchronizerId, hashCid("B")) -> Some(ActivationRef(1234, 12340)),
            SynCon(someSynchronizerId, hashCid("C")) -> Some(ActivationRef(12345, 123450)),
          ),
          batchTraceContext = TraceContext.empty,
        )
      )
      .batch should contain theSameElementsInOrderAs Vector(
      someEventDeactivate.copy(
        synchronizer_id = someSynchronizerId2,
        contract_id = hashCid("A"),
        deactivated_event_sequential_id = Some(123),
        internal_contract_id = Some(1230),
      ),
      someEventDeactivate.copy(
        synchronizer_id = someSynchronizerId,
        contract_id = hashCid("C"),
        deactivated_event_sequential_id = Some(12345),
        internal_contract_id = Some(123450),
      ),
      someEventDeactivate.copy(
        synchronizer_id = someSynchronizerId,
        contract_id = hashCid("B"),
        deactivated_event_sequential_id = Some(1234),
        internal_contract_id = Some(12340),
      ),
      someEventDeactivate.copy(
        synchronizer_id = someSynchronizerId,
        contract_id = hashCid("B"),
        deactivated_event_sequential_id = Some(10000),
        internal_contract_id = Some(100000),
      ),
    )
  }

  it should "report warning, but succeed, if activation is missing" in {
    loggerFactory.assertLogs(
      LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(Level.WARN)
    )(
      ParallelIndexerSubscription
        .refillMissingDeactivatedActivations(logger)(
          Batch(
            ledgerEnd = previousLedgerEnd,
            batch = Vector(
              someEventDeactivate.copy(
                synchronizer_id = someSynchronizerId2,
                contract_id = hashCid("A"),
                deactivated_event_sequential_id = None,
                event_type = 3,
              )
            ),
            batchSize = 1,
            offsetsUpdates = Vector.empty,
            activeContracts = EmptyActiveContracts,
            missingDeactivatedActivations = Map(
              SynCon(someSynchronizerId2, hashCid("A")) -> None,
              SynCon(someSynchronizerId, hashCid("B")) -> Some(ActivationRef(1234, 12340)),
            ),
            batchTraceContext = TraceContext.empty,
          )
        )
        .batch should contain theSameElementsInOrderAs Vector(
        someEventDeactivate.copy(
          synchronizer_id = someSynchronizerId2,
          contract_id = hashCid("A"),
          deactivated_event_sequential_id = None,
          internal_contract_id = None,
          event_type = 3,
        )
      ),
      _.warningMessage should include(
        s"Activation is missing for a deactivation for deactivated event with type:ConsumingExercise offset:1 nodeId:1 for synchronizerId:$someSynchronizerId2 contractId:${hashCid("A")}."
      ),
    )
  }

  it should "report error and fail, if activation was not even requested" in {
    loggerFactory.assertInternalError[IllegalStateException](
      ParallelIndexerSubscription.refillMissingDeactivatedActivations(logger)(
        Batch(
          ledgerEnd = previousLedgerEnd,
          batch = Vector(
            someEventDeactivate.copy(
              synchronizer_id = someSynchronizerId2,
              contract_id = hashCid("A"),
              deactivated_event_sequential_id = None,
              event_type = 3,
            )
          ),
          batchSize = 1,
          offsetsUpdates = Vector.empty,
          activeContracts = EmptyActiveContracts,
          missingDeactivatedActivations = Map(
            SynCon(someSynchronizerId, hashCid("B")) -> Some(ActivationRef(1234, 12340))
          ),
          batchTraceContext = TraceContext.empty,
        )
      ),
      _.getMessage should include(
        s"Programming error: deactivation reference is missing for deactivated event with type:ConsumingExercise offset:1 nodeId:1 for synchronizerId:$someSynchronizerId2 contractId:${hashCid("A")}, but lookup was not even initiated."
      ),
    )
  }

  behavior of "dbPrepare"

  it should "apply missing deactivated activations" in {
    def lastActivations(@unused _synchronizerContracts: Iterable[(SynchronizerId, Long)])(
        @unused _connection: Connection
    ): Map[(SynchronizerId, Long), Long] = Map(
      (someSynchronizerId, 5L) -> 2L,
      (someSynchronizerId2, 15L) -> 4L,
    )

    def resolveInternalContractIds(@unused _tc: TraceContext)(
        @unused _contractIds: Iterable[ContractId]
    ): Future[Map[ContractId, Long]] = Future.successful {
      Map(
        hashCid("#1") -> 5L,
        hashCid("#2") -> 7L,
        hashCid("#3") -> 15L,
      )
    }

    val dtos = Vector(someEventActivate)
    val ledgerEnd = LedgerEnd(
      lastOffset = offset(2),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
      lastPublicationTime = CantonTimestamp.MinValue,
    )
    val inBatch = Batch(
      ledgerEnd = ledgerEnd,
      batchTraceContext = TraceContext.empty,
      batch = dtos,
      batchSize = dtos.size,
      offsetsUpdates = Vector.empty,
      missingDeactivatedActivations = Map(
        SynCon(someSynchronizerId, hashCid("#1")) -> None,
        SynCon(someSynchronizerId, hashCid("#2")) -> None, // not in last activations
        SynCon(someSynchronizerId2, hashCid("#3")) -> None,
      ),
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
    )

    val outBatchF = ParallelIndexerSubscription.dbPrepare(
      lastActivations,
      mockDbDispatcher(new TestConnection),
      resolveInternalContractIds,
      metrics,
      logger,
    )(inBatch)

    outBatchF.futureValue shouldBe
      Batch(
        ledgerEnd = ledgerEnd,
        batchTraceContext = TraceContext.empty,
        batch = dtos,
        batchSize = dtos.size,
        offsetsUpdates = Vector.empty,
        missingDeactivatedActivations = Map(
          SynCon(someSynchronizerId, hashCid("#1")) -> Some(ActivationRef(2L, 5L)),
          SynCon(someSynchronizerId, hashCid("#2")) -> None,
          SynCon(someSynchronizerId2, hashCid("#3")) -> Some(ActivationRef(4L, 15L)),
        ),
        activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
      )
  }

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerSubscription.batcher(
      batchF = _ => "bumm",
      logger = logger,
    )(
      Batch(
        ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
        batchTraceContext = TraceContext.empty,
        batch = Vector(
          someParty,
          someParty,
          someParty,
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
        missingDeactivatedActivations = Map.empty,
        activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
      )
    )
    result shouldBe Batch(
      ledgerEnd = ZeroLedgerEnd.copy(lastOffset = offset(2)),
      batchTraceContext = TraceContext.empty,
      batch = "bumm",
      batchSize = 3,
      offsetsUpdates = offsetsAndUpdates,
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
    )
  }

  behavior of "ingester"

  it should "apply ingestFunction and cleanUnusedBatch" in {
    val connection = new TestConnection

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
      batchTraceContext = TraceContext.empty,
      batch = batchPayload,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
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
        mockDbDispatcher(connection),
        metrics,
        logger,
      )(inBatch)

    val outBatch = Await.result(outBatchF, 10.seconds)

    outBatch shouldBe
      Batch(
        ledgerEnd = ledgerEnd,
        batchTraceContext = TraceContext.empty,
        batch = zeroDbBatch,
        batchSize = 0,
        offsetsUpdates = Vector.empty,
        missingDeactivatedActivations = Map.empty,
        activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
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
      batchTraceContext = TraceContext.empty,
      batch = "Some batch payload",
      batchSize = 0,
      offsetsUpdates = Vector.empty,
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
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
      batchTraceContext = TraceContext.empty,
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
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
    ),
    Batch(
      ledgerEnd = LedgerEnd(
        lastOffset = offset(20),
        lastEventSeqId = 2020,
        lastStringInterningId = 320,
        lastPublicationTime = CantonTimestamp.ofEpochMicro(25),
      ),
      batchTraceContext = TraceContext.empty,
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
      missingDeactivatedActivations = Map.empty,
      activeContracts = ParallelIndexerSubscription.EmptyActiveContracts,
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

  it should "throw if offsets are not in a strictly increasing order" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe)

        testSink.request(3)
        testSink.expectNextN(offsetsUpdates.take(2))

        throw testSink.expectError()
      },
      _.getMessage shouldBe "Monotonic Offset violation detected from Offset(3) to Offset(2)",
    )

  it should "throw if offsets are not in a strictly increasing compared to the initial offset" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe)

        testSink.request(1)
        throw testSink.expectError()
      },
      _.getMessage shouldBe "Monotonic Offset violation detected from Offset(2) to Offset(1)",
    )

  it should "throw if sequenced timestamps decrease" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(2)
        testSink.expectNextN(offsetsUpdates.take(1))

        throw testSink.expectError()
      },
      _.getMessage should include regex raw"Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(2\)",
    )

  it should "throw if sequenced timestamps decrease compared to clean synchronizer index" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(1)
        throw testSink.expectError()
      },
      _.getMessage should include regex raw"Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(1\)",
    )

  it should "throw if sequenced timestamps not increasing" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(2)
        testSink.expectNextN(offsetsUpdates.take(1))

        throw testSink.expectError()
      },
      _.getMessage should include regex raw"Monotonicity violation detected: sequencer timestamp did not increase from .* to .* at offset Offset\(2\)",
    )

  it should "throw if sequenced timestamps not increasing compared to clean synchronizer index" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(1)
        throw testSink.expectError()
      },
      _.getMessage should include regex raw"Monotonicity violation detected: sequencer timestamp did not increase from .* to .* at offset Offset\(1\)",
    )

  it should "throw if repair counters decrease" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
        val offsetsUpdates: Vector[(Offset, Update)] = Vector(
          offset(1L) -> repairUpdate(CantonTimestamp.Epoch, RepairCounter(15L)),
          offset(2L) -> repairUpdate(CantonTimestamp.Epoch, RepairCounter(13L)),
        )

        val testSink = Source(offsetsUpdates)
          .via(
            ParallelIndexerSubscription.monotonicityValidator(
              initialOffset = None,
              loadPreviousState = _ => Future.successful(None),
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(2)
        testSink.expectNextN(offsetsUpdates.take(1))

        throw testSink.expectError()
      },
      _.getMessage should include regex
        raw"Monotonicity violation detected: repair index decreases from .* to .* at offset Offset\(2\)",
    )

  it should "throw if repair counters decrease compared to clean synchronizer index" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(1)
        throw testSink.expectError()
      },
      _.getMessage should include regex
        raw"Monotonicity violation detected: repair index decreases from .* to .* at offset Offset\(1\)",
    )

  it should "throw if record time decreases for floating events" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(3)
        testSink.expectNextN(offsetsUpdates.take(2))

        throw testSink.expectError()
      },
      _.getMessage should include regex
        raw"Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(3\)",
    )

  it should "throw if record time decreases for floating events compared to clean synchronizer index" in
    loggerFactory.assertInternalError[IllegalStateException](
      {
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
            )(logger)
          )
          .runWith(TestSink.probe[(Offset, Update)])

        testSink.request(1)
        throw testSink.expectError()
      },
      _.getMessage should include regex
        raw"Monotonicity violation detected: record time decreases from .* to .* at offset Offset\(1\)",
    )

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
      updateId = TestUpdateId("15000"),
      contractAuthenticationData = Map.empty,
      representativePackageIds = RepresentativePackageIds.Empty,
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      repairCounter = repairCounter,
      recordTime = recordTime,
      internalContractIds = Map.empty,
    )(TraceContext.empty)

  def floatingUpdate(recordTime: CantonTimestamp): Update =
    TopologyTransactionEffective(
      updateId = TestUpdateId("16000"),
      events = Set.empty,
      synchronizerId = SynchronizerId.tryFromString("x::synchronizer"),
      effectiveTime = recordTime,
    )(TraceContext.empty)
}
