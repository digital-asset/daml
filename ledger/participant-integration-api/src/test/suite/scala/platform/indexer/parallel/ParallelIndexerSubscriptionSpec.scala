// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.TransactionNodeStatistics.EmptyActions
import com.daml.lf.transaction.{
  CommittedTransaction,
  TransactionNodeStatistics,
  TransactionVersion,
  VersionedTransaction,
}
import com.daml.logging.LoggingContext
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.indexer.ha.TestConnection
import com.daml.platform.indexer.parallel.ParallelIndexerSubscription.Batch
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{DbDto, ParameterStorageBackend}
import com.daml.platform.store.dao.DbDispatcher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.{Await, Future}

class ParallelIndexerSubscriptionSpec extends AnyFlatSpec with Matchers {

  private implicit val lc: LoggingContext = LoggingContext.ForTesting

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

  private val someTime = Instant.now

  private val somePackageUploadRejected = state.Update.PublicPackageUploadRejected(
    submissionId = Ref.SubmissionId.assertFromString("abc"),
    recordTime = Timestamp.assertFromInstant(someTime),
    rejectionReason = "reason",
  )

  private def offset(s: String): Offset = Offset.fromHexString(Ref.HexString.assertFromString(s))

  private val metrics = Metrics.ForTesting

  private val someEventCreated = DbDto.EventCreate(
    event_offset = None,
    transaction_id = None,
    ledger_effective_time = None,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = None,
    event_id = None,
    contract_id = "1",
    template_id = None,
    flat_event_witnesses = Set.empty,
    tree_event_witnesses = Set.empty,
    create_argument = None,
    create_signatories = None,
    create_observers = None,
    create_agreement_text = None,
    create_key_value = None,
    create_key_hash = None,
    create_argument_compression = None,
    create_key_value_compression = None,
    event_sequential_id = 0,
    driver_metadata = None,
  )

  private val someEventExercise = DbDto.EventExercise(
    consuming = true,
    event_offset = None,
    transaction_id = None,
    ledger_effective_time = None,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = None,
    event_id = None,
    contract_id = "1",
    template_id = None,
    flat_event_witnesses = Set.empty,
    tree_event_witnesses = Set.empty,
    create_key_value = None,
    exercise_choice = None,
    exercise_argument = None,
    exercise_result = None,
    exercise_actors = None,
    exercise_child_event_ids = None,
    create_key_value_compression = None,
    exercise_argument_compression = None,
    exercise_result_compression = None,
    event_sequential_id = 0,
  )

  private val someEventDivulgence = DbDto.EventDivulgence(
    event_offset = None,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    contract_id = "1",
    template_id = None,
    tree_event_witnesses = Set.empty,
    create_argument = None,
    create_argument_compression = None,
    event_sequential_id = 0,
  )

  private val offsetsAndUpdates =
    Vector("00", "01", "02")
      .map(offset)
      .zip(
        Vector(
          somePackageUploadRejected,
          somePackageUploadRejected
            .copy(recordTime = somePackageUploadRejected.recordTime.addMicros(1000)),
          somePackageUploadRejected
            .copy(recordTime = somePackageUploadRejected.recordTime.addMicros(2000)),
        )
      )

  behavior of "inputMapper"

  it should "provide required Batch in happy path case" in {
    val actual = ParallelIndexerSubscription.inputMapper(
      metrics = metrics,
      toDbDto = _ => _ => Iterator(someParty, someParty),
      toMeteringDbDto = _ => Vector.empty,
    )(lc)(
      List(
        Offset.fromHexString(Ref.HexString.assertFromString("00")),
        Offset.fromHexString(Ref.HexString.assertFromString("01")),
        Offset.fromHexString(Ref.HexString.assertFromString("02")),
      ).zip(offsetsAndUpdates.map(_._2))
    )
    val expected = Batch[Vector[DbDto]](
      lastOffset = offset("02"),
      lastSeqEventId = 0,
      lastStringInterningId = 0,
      lastRecordTime = someTime.plusMillis(2).toEpochMilli,
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
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someRecordTime = Time.Timestamp.assertFromString("2000-01-01T00:00:00.000000Z")

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = Some(statistics),
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
      optCompletionInfo = Some(someCompletionInfo),
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(
        VersionedTransaction(TransactionVersion.VDev, Map.empty, ImmArray.empty)
      ),
      transactionId = Ref.TransactionId.assertFromString("TransactionId"),
      recordTime = someRecordTime,
      divulgedContracts = List.empty,
      blindingInfo = None,
      contractMetadata = Map.empty,
    )

    val expected: Vector[DbDto.TransactionMetering] = Vector(
      DbDto.TransactionMetering(
        application_id = applicationId,
        action_count = statistics.committed.actions + statistics.rolledBack.actions,
        metering_timestamp = timestamp,
        ledger_offset = offset,
      )
    )

    val actual: Vector[DbDto.TransactionMetering] = ParallelIndexerSubscription
      .inputMapper(
        metrics = metrics,
        toDbDto = _ => _ => Iterator.empty,
        toMeteringDbDto = _ => expected,
      )(lc)(
        List(
          (Offset.fromHexString(offset), someTransactionAccepted)
        )
      )
      .batch
      .asInstanceOf[Vector[DbDto.TransactionMetering]]

    actual shouldBe expected

  }

  behavior of "seqMapperZero"

  it should "provide required Batch in happy path case" in {
    ParallelIndexerSubscription.seqMapperZero(123, 234) shouldBe Batch(
      lastOffset = null,
      lastSeqEventId = 123,
      lastStringInterningId = 234,
      lastRecordTime = 0,
      batch = Vector.empty,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )
  }

  behavior of "seqMapper"

  it should "assign sequence ids correctly, and populate string-interning entries correctly in happy path case" in {
    val result: Batch[Vector[DbDto]] = ParallelIndexerSubscription.seqMapper(
      internize = _.zipWithIndex.map(x => x._2 -> x._2.toString).take(2),
      metrics,
    )(
      previous = ParallelIndexerSubscription.seqMapperZero(15, 26),
      current = Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
        lastStringInterningId = 0,
        lastRecordTime = someTime.toEpochMilli,
        batch = Vector(
          someParty,
          someEventDivulgence,
          someParty,
          someEventCreated,
          DbDto.IdFilterCreateStakeholder(0L, "", ""),
          DbDto.IdFilterCreateNonStakeholderInformee(0L, ""),
          DbDto.IdFilterConsumingStakeholder(0L, "", ""),
          DbDto.IdFilterConsumingNonStakeholderInformee(0L, ""),
          DbDto.IdFilterNonConsumingInformee(0L, ""),
          someEventCreated,
          someEventCreated,
          DbDto.TransactionMeta("", "", 0L, 0L),
          someParty,
          someEventExercise,
          DbDto.TransactionMeta("", "", 0L, 0L),
          someParty,
        ),
        batchSize = 3,
        offsetsUpdates = offsetsAndUpdates,
      ),
    )
    import scala.util.chaining._

    result.lastSeqEventId shouldBe 20
    result.lastStringInterningId shouldBe 1
    result.batch(1).asInstanceOf[DbDto.EventDivulgence].event_sequential_id shouldBe 16
    result.batch(3).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 17
    result.batch(4).asInstanceOf[DbDto.IdFilterCreateStakeholder].event_sequential_id shouldBe 17
    result
      .batch(5)
      .asInstanceOf[DbDto.IdFilterCreateNonStakeholderInformee]
      .event_sequential_id shouldBe 17
    result.batch(6).asInstanceOf[DbDto.IdFilterConsumingStakeholder].event_sequential_id shouldBe 17
    result
      .batch(7)
      .asInstanceOf[DbDto.IdFilterConsumingNonStakeholderInformee]
      .event_sequential_id shouldBe 17
    result.batch(8).asInstanceOf[DbDto.IdFilterNonConsumingInformee].event_sequential_id shouldBe 17
    result.batch(11).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 16L
      transactionMeta.event_sequential_id_last shouldBe 19L
    }
    result.batch(13).asInstanceOf[DbDto.EventExercise].event_sequential_id shouldBe 20
    result.batch(14).asInstanceOf[DbDto.TransactionMeta].tap { transactionMeta =>
      transactionMeta.event_sequential_id_first shouldBe 20L
      transactionMeta.event_sequential_id_last shouldBe 20L
    }
    result.batch(16).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 0
    result.batch(16).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "0"
    result.batch(17).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 1
    result.batch(17).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "1"
  }

  it should "preserve sequence id if nothing to assign" in {
    val result = ParallelIndexerSubscription.seqMapper(_ => Nil, metrics)(
      ParallelIndexerSubscription.seqMapperZero(15, 25),
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
        lastStringInterningId = 0,
        lastRecordTime = someTime.toEpochMilli,
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

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerSubscription.batcher(
      batchF = _ => "bumm"
    )(
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
        lastRecordTime = someTime.toEpochMilli,
        lastStringInterningId = 0,
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
      lastOffset = offset("02"),
      lastSeqEventId = 0,
      lastStringInterningId = 0,
      lastRecordTime = someTime.toEpochMilli,
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
          loggingContext: LoggingContext
      ): Future[T] =
        Future.successful(sql(connection))
    }

    val batchPayload = "Some batch payload"

    val ingestFunction: (Connection, String) => Unit = {
      case (`connection`, `batchPayload`) => ()
      case other => fail(s"Unexpected: $other")
    }

    val inBatch = Batch(
      lastOffset = offset("05"),
      lastSeqEventId = 2000,
      lastStringInterningId = 300,
      lastRecordTime = someTime.toEpochMilli,
      batch = batchPayload,
      batchSize = 0,
      offsetsUpdates = Vector.empty,
    )

    val zeroDbBatch = "zero"
    val outBatchF =
      ParallelIndexerSubscription.ingester(ingestFunction, "zero", dbDispatcher, metrics)(
        LoggingContext.ForTesting
      )(inBatch)

    val outBatch = Await.result(outBatchF, 10.seconds)

    outBatch shouldBe
      Batch(
        lastOffset = offset("05"),
        lastSeqEventId = 2000,
        lastStringInterningId = 300,
        lastRecordTime = someTime.toEpochMilli,
        batch = zeroDbBatch,
        batchSize = 0,
        offsetsUpdates = Vector.empty,
      )
  }

  behavior of "ingestTail"

  it should "apply ingestTailFunction on the last batch and forward the batch of batches" in {
    val connection = new TestConnection
    val dbDispatcher = new DbDispatcher {
      override def executeSql[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
          loggingContext: LoggingContext
      ): Future[T] =
        Future.successful(sql(connection))
    }

    val ledgerEnd = ParameterStorageBackend.LedgerEnd(
      lastOffset = offset("05"),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
    )

    val secondBatchLedgerEnd = ParameterStorageBackend.LedgerEnd(
      lastOffset = offset("06"),
      lastEventSeqId = 3000,
      lastStringInterningId = 400,
    )

    val ingestTailFunction: LedgerEnd => Connection => Unit = {
      case `secondBatchLedgerEnd` => {
        case `connection` => ()
        case otherConnection => fail(s"Unexpected connection: $otherConnection")
      }
      case otherLedgerEnd => fail(s"Unexpected ledger end: $otherLedgerEnd")
    }

    val batch = Batch(
      lastOffset = ledgerEnd.lastOffset,
      lastSeqEventId = ledgerEnd.lastEventSeqId,
      lastStringInterningId = ledgerEnd.lastStringInterningId,
      lastRecordTime = someTime.toEpochMilli,
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
      ),
    )

    val outBatchF =
      ParallelIndexerSubscription.ingestTail(ingestTailFunction, dbDispatcher, metrics)(
        LoggingContext.ForTesting
      )(batchOfBatches)

    val outBatch = Await.result(outBatchF, 10.seconds)
    outBatch shouldBe batchOfBatches
  }

  behavior of "ledgerEndFromBatch"

  it should "populate correct ledger-end from batch in happy path case" in {
    ParallelIndexerSubscription.ledgerEndFrom(
      Batch(
        lastOffset = offset("05"),
        lastSeqEventId = 2000,
        lastStringInterningId = 300,
        lastRecordTime = someTime.toEpochMilli,
        batch = "zero",
        batchSize = 0,
        offsetsUpdates = Vector.empty,
      )
    ) shouldBe ParameterStorageBackend.LedgerEnd(
      lastOffset = offset("05"),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
    )
  }

}
