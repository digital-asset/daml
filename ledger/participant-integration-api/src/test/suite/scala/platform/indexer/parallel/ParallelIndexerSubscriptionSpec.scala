// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import com.codahale.metrics.MetricRegistry
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
import com.daml.metrics.Metrics
import com.daml.platform.indexer.parallel.ParallelIndexerSubscription.Batch
import com.daml.platform.store.backend.{DbDto, ParameterStorageBackend}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ParallelIndexerSubscriptionSpec extends AnyFlatSpec with Matchers {

  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  private val DbDtoEq: org.scalactic.Equality[DbDto] = {
    case (a: DbDto, b: DbDto) =>
      (a.productPrefix === b.productPrefix) &&
        (a.productArity == b.productArity) &&
        (a.productIterator zip b.productIterator).forall {
          case (x: Array[_], y: Array[_]) => x sameElements y
          case (Some(x: Array[_]), Some(y: Array[_])) => x sameElements y
          case (x, y) => x === y
        }
    case (_, _) => false
  }

  private val DbDtoSeqEq: org.scalactic.Equality[Seq[DbDto]] = {
    case (a: Seq[_], b: Seq[_]) =>
      a.size == b.size && a.zip(b).forall({ case (x, y) => DbDtoEq.areEqual(x, y) })
    case (_, _) => false
  }

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

  private val metrics = new Metrics(new MetricRegistry())

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

  behavior of "DbDtoEq"

  it should "compare DbDto when used with `decided` keyword" in {

    val dto0 = DbDto.ConfigurationEntry(
      ledger_offset = "",
      recorded_at = 0,
      submission_id = "",
      typ = "",
      configuration = Array[Byte](),
      rejection_reason = None,
    )

    val dto1 = dto0.copy()
    val dto2 = dto0.copy()

    dto0 should equal(dto0) // Works due to object equality shortcut
    dto1 shouldNot equal(dto2) // As equality is overridden to be false with DbDto
    dto1 should equal(dto2)(decided by DbDtoEq)
    List(dto1) should equal(List(dto2))(decided by DbDtoSeqEq)

  }

  behavior of "inputMapper"

  it should "provide required Batch in happy path case" in {
    val actual = ParallelIndexerSubscription.inputMapper(
      metrics = metrics,
      toDbDto = _ => _ => Iterator(someParty, someParty),
    )(lc)(
      List(
        (
          (Offset.fromHexString(Ref.HexString.assertFromString("00")), somePackageUploadRejected),
          1000000001,
        ),
        (
          (
            Offset.fromHexString(Ref.HexString.assertFromString("01")),
            somePackageUploadRejected.copy(recordTime =
              somePackageUploadRejected.recordTime.addMicros(1000)
            ),
          ),
          1000000002,
        ),
        (
          (
            Offset.fromHexString(Ref.HexString.assertFromString("02")),
            somePackageUploadRejected.copy(recordTime =
              somePackageUploadRejected.recordTime.addMicros(2000)
            ),
          ),
          1000000003,
        ),
      )
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
      averageStartTime = 1000000001,
      offsets = Vector("00", "01", "02").map(offset),
    )
    actual shouldBe expected
  }

  behavior of "inputMapper transaction metering"

  {

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
    )

    it should "extract transaction metering" in {
      val actual: Vector[DbDto.TransactionMetering] = ParallelIndexerSubscription
        .inputMapper(
          metrics = metrics,
          toDbDto = _ => _ => Iterator.empty,
        )(lc)(
          List(
            (
              (Offset.fromHexString(offset), someTransactionAccepted),
              timestamp,
            )
          )
        )
        .batch
        .asInstanceOf[Vector[DbDto.TransactionMetering]]

      val expected: Vector[DbDto.TransactionMetering] = Vector(
        DbDto.TransactionMetering(
          application_id = applicationId,
          action_count = statistics.committed.actions + statistics.rolledBack.actions,
          from_timestamp = timestamp,
          to_timestamp = timestamp,
          from_ledger_offset = offset,
          to_ledger_offset = offset,
        )
      )

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

    it should "aggregate transaction metering across batch" in {

      val metering = DbDto.TransactionMetering(
        application_id = applicationId,
        action_count = 2 * (statistics.committed.actions + statistics.rolledBack.actions),
        from_timestamp = timestamp - 1,
        to_timestamp = timestamp + 1,
        from_ledger_offset = Ref.HexString.assertFromString("01"),
        to_ledger_offset = Ref.HexString.assertFromString("03"),
      )

      val expected: Vector[DbDto.TransactionMetering] = Vector(metering)

      val actual: Vector[DbDto.TransactionMetering] = ParallelIndexerSubscription
        .inputMapper(
          metrics = metrics,
          toDbDto = _ => _ => Iterator.empty,
        )(lc)(
          List(
            (
              (
                Offset.fromHexString(Ref.HexString.assertFromString(metering.from_ledger_offset)),
                someTransactionAccepted,
              ),
              metering.from_timestamp,
            ),
            (
              (
                Offset.fromHexString(Ref.HexString.assertFromString(metering.to_ledger_offset)),
                someTransactionAccepted,
              ),
              metering.to_timestamp,
            ),
          )
        )
        .batch
        .asInstanceOf[Vector[DbDto.TransactionMetering]]

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

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
      averageStartTime = 0,
      offsets = Vector.empty,
    )
  }

  behavior of "seqMapper"

  it should "assign sequence ids correctly, and populate string-interning entries correctly in happy path case" in {
    val result = ParallelIndexerSubscription.seqMapper(
      _.zipWithIndex.map(x => x._2 -> x._2.toString).take(2),
      metrics,
    )(
      ParallelIndexerSubscription.seqMapperZero(15, 26),
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
        lastStringInterningId = 0,
        lastRecordTime = someTime.toEpochMilli,
        batch = Vector(
          someParty,
          someEventDivulgence,
          someParty,
          someEventCreated,
          DbDto.CreateFilter(0L, "", ""),
          DbDto.CreateFilter(0L, "", ""),
          someParty,
          someEventExercise,
          someParty,
        ),
        batchSize = 3,
        averageStartTime = 1000000001,
        offsets = Vector("00", "01", "02").map(offset),
      ),
    )
    result.lastSeqEventId shouldBe 18
    result.lastStringInterningId shouldBe 1
    result.averageStartTime should be > System.nanoTime() - 1000000000
    result.batch(1).asInstanceOf[DbDto.EventDivulgence].event_sequential_id shouldBe 16
    result.batch(3).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 17
    result.batch(4).asInstanceOf[DbDto.CreateFilter].event_sequential_id shouldBe 17
    result.batch(5).asInstanceOf[DbDto.CreateFilter].event_sequential_id shouldBe 17
    result.batch(7).asInstanceOf[DbDto.EventExercise].event_sequential_id shouldBe 18
    result.batch(9).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 0
    result.batch(9).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "0"
    result.batch(10).asInstanceOf[DbDto.StringInterningDto].internalId shouldBe 1
    result.batch(10).asInstanceOf[DbDto.StringInterningDto].externalString shouldBe "1"
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
        averageStartTime = 1000000001,
        offsets = Vector("00", "01", "02").map(offset),
      ),
    )
    result.lastSeqEventId shouldBe 15
    result.lastStringInterningId shouldBe 25
  }

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerSubscription.batcher(
      batchF = _ => "bumm",
      metrics = metrics,
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
        averageStartTime = 1000000001,
        offsets = Vector("00", "01", "02").map(offset),
      )
    )
    result shouldBe Batch(
      lastOffset = offset("02"),
      lastSeqEventId = 0,
      lastStringInterningId = 0,
      lastRecordTime = someTime.toEpochMilli,
      batch = "bumm",
      batchSize = 3,
      averageStartTime = result.averageStartTime,
      offsets = Vector("00", "01", "02").map(offset),
    )
    result.averageStartTime should be > System.nanoTime() - 1000000000
  }

  behavior of "tailer"

  it should "propagate last ledger-end correctly in happy path case" in {
    ParallelIndexerSubscription.tailer("zero")(
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 1000,
        lastStringInterningId = 200,
        lastRecordTime = someTime.toEpochMilli - 1000,
        batch = "bumm1",
        batchSize = 3,
        averageStartTime = 8,
        offsets = Vector("00", "01", "02").map(offset),
      ),
      Batch(
        lastOffset = offset("05"),
        lastSeqEventId = 2000,
        lastStringInterningId = 210,
        lastRecordTime = someTime.toEpochMilli,
        batch = "bumm2",
        batchSize = 3,
        averageStartTime = 10,
        offsets = Vector("03", "04", "05").map(offset),
      ),
    ) shouldBe Batch(
      lastOffset = offset("05"),
      lastSeqEventId = 2000,
      lastStringInterningId = 210,
      lastRecordTime = someTime.toEpochMilli,
      batch = "zero",
      batchSize = 0,
      averageStartTime = 0,
      offsets = Vector.empty,
    )
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
        averageStartTime = 0,
        offsets = Vector.empty,
      )
    ) shouldBe ParameterStorageBackend.LedgerEnd(
      lastOffset = offset("05"),
      lastEventSeqId = 2000,
      lastStringInterningId = 300,
    )
  }

}
