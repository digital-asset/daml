// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.indexer.parallel.ParallelIndexerFactory.Batch
import com.daml.platform.store.backend.{DbDto, ParameterStorageBackend}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParallelIndexerFactorySpec extends AnyFlatSpec with Matchers {

  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  private val someParty = DbDto.Party(
    party = "party",
    display_name = None,
    explicit = true,
    ledger_offset = None,
    is_local = true,
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

  behavior of "inputMapper"

  it should "provide required Batch in happy path case" in {
    val actual = ParallelIndexerFactory.inputMapper(
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

  behavior of "seqMapperZero"

  it should "provide required Batch in happy path case" in {
    ParallelIndexerFactory.seqMapperZero(123) shouldBe Batch(
      lastOffset = null,
      lastSeqEventId = 123,
      lastRecordTime = 0,
      batch = Vector.empty,
      batchSize = 0,
      averageStartTime = 0,
      offsets = Vector.empty,
    )
  }

  behavior of "seqMapper"

  it should "assign sequence ids correctly in happy path case" in {
    val result = ParallelIndexerFactory.seqMapper(metrics)(
      ParallelIndexerFactory.seqMapperZero(15),
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
        lastRecordTime = someTime.toEpochMilli,
        batch = Vector(
          someParty,
          someEventDivulgence,
          someParty,
          someEventCreated,
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
    result.averageStartTime should be > System.nanoTime() - 1000000000
    result.batch(1).asInstanceOf[DbDto.EventDivulgence].event_sequential_id shouldBe 16
    result.batch(3).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 17
    result.batch(5).asInstanceOf[DbDto.EventExercise].event_sequential_id shouldBe 18
  }

  it should "preserve sequence id if nothing to assign" in {
    val result = ParallelIndexerFactory.seqMapper(metrics)(
      ParallelIndexerFactory.seqMapperZero(15),
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
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
  }

  behavior of "batcher"

  it should "batch correctly in happy path case" in {
    val result = ParallelIndexerFactory.batcher(
      batchF = _ => "bumm",
      metrics = metrics,
    )(
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 0,
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
      )
    )
    result shouldBe Batch(
      lastOffset = offset("02"),
      lastSeqEventId = 0,
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
    ParallelIndexerFactory.tailer("zero")(
      Batch(
        lastOffset = offset("02"),
        lastSeqEventId = 1000,
        lastRecordTime = someTime.toEpochMilli - 1000,
        batch = "bumm1",
        batchSize = 3,
        averageStartTime = 8,
        offsets = Vector("00", "01", "02").map(offset),
      ),
      Batch(
        lastOffset = offset("05"),
        lastSeqEventId = 2000,
        lastRecordTime = someTime.toEpochMilli,
        batch = "bumm2",
        batchSize = 3,
        averageStartTime = 10,
        offsets = Vector("03", "04", "05").map(offset),
      ),
    ) shouldBe Batch(
      lastOffset = offset("05"),
      lastSeqEventId = 2000,
      lastRecordTime = someTime.toEpochMilli,
      batch = "zero",
      batchSize = 0,
      averageStartTime = 0,
      offsets = Vector.empty,
    )
  }

  behavior of "ledgerEndFromBatch"

  it should "populate correct ledger-end from batch in happy path case" in {
    ParallelIndexerFactory.ledgerEndFrom(
      Batch(
        lastOffset = offset("05"),
        lastSeqEventId = 2000,
        lastRecordTime = someTime.toEpochMilli,
        batch = "zero",
        batchSize = 0,
        averageStartTime = 0,
        offsets = Vector.empty,
      )
    ) shouldBe ParameterStorageBackend.LedgerEnd(
      lastOffset = offset("05"),
      lastEventSeqId = 2000,
    )
  }
}
