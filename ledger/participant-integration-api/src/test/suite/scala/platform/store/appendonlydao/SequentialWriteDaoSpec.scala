// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.SequentialWriteDaoSpec._
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequentialWriteDaoSpec extends AnyFlatSpec with Matchers {

  behavior of "SequentialWriteDaoImpl"

  it should "store correctly in a happy path case" in {
    val storageBackendCaptor = new StorageBackendCaptor(Some(LedgerEnd(Offset.beforeBegin, 5)))
    val testee = SequentialWriteDaoImpl(
      storageBackend = storageBackendCaptor,
      updateToDbDtos = updateToDbDtoFixture,
    )
    testee.store(someConnection, offset("01"), singlePartyFixture)
    testee.store(someConnection, offset("02"), allEventsFixture)
    testee.store(someConnection, offset("03"), None)
    testee.store(someConnection, offset("04"), partyAndCreateFixture)

    storageBackendCaptor.captured(0) shouldBe someParty
    storageBackendCaptor.captured(1) shouldBe LedgerEnd(offset("01"), 5)
    storageBackendCaptor.captured(2).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(3)
      .asInstanceOf[DbDto.EventExercise]
      .event_sequential_id shouldBe 7
    storageBackendCaptor
      .captured(4)
      .asInstanceOf[DbDto.EventDivulgence]
      .event_sequential_id shouldBe 8
    storageBackendCaptor.captured(5) shouldBe LedgerEnd(offset("02"), 8)
    storageBackendCaptor.captured(6) shouldBe LedgerEnd(offset("03"), 8)
    storageBackendCaptor.captured(7) shouldBe someParty
    storageBackendCaptor.captured(8).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 9
    storageBackendCaptor.captured(9) shouldBe LedgerEnd(offset("04"), 9)
    storageBackendCaptor.captured should have size 10
  }

  it should "start event_seq_id from 1" in {
    val storageBackendCaptor = new StorageBackendCaptor(None)
    val testee = SequentialWriteDaoImpl(
      storageBackend = storageBackendCaptor,
      updateToDbDtos = updateToDbDtoFixture,
    )
    testee.store(someConnection, offset("03"), None)
    testee.store(someConnection, offset("04"), partyAndCreateFixture)

    storageBackendCaptor.captured(0) shouldBe LedgerEnd(offset("03"), 0)
    storageBackendCaptor.captured(1) shouldBe someParty
    storageBackendCaptor.captured(2).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 1
    storageBackendCaptor.captured(3) shouldBe LedgerEnd(offset("04"), 1)
    storageBackendCaptor.captured should have size 4
  }

  class StorageBackendCaptor(initialLedgerEnd: Option[ParameterStorageBackend.LedgerEnd])
      extends IngestionStorageBackend[Vector[DbDto]]
      with ParameterStorageBackend {

    var captured: Vector[Any] = Vector.empty

    override def batch(dbDtos: Vector[DbDto]): Vector[DbDto] = dbDtos

    override def insertBatch(connection: Connection, batch: Vector[DbDto]): Unit = synchronized {
      connection shouldBe someConnection
      captured = captured ++ batch
    }

    override def initializeIngestion(
        connection: Connection
    ): Option[ParameterStorageBackend.LedgerEnd] =
      throw new UnsupportedOperationException

    override def updateLedgerEnd(
        params: ParameterStorageBackend.LedgerEnd
    )(connection: Connection): Unit =
      synchronized {
        connection shouldBe someConnection
        captured = captured :+ params
      }

    private var ledgerEndCalled = false
    override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
      synchronized {
        connection shouldBe someConnection
        ledgerEndCalled shouldBe false
        ledgerEndCalled = true
        initialLedgerEnd
      }

    override def initializeParameters(params: ParameterStorageBackend.IdentityParams)(
        connection: Connection
    )(implicit loggingContext: LoggingContext): Unit =
      throw new UnsupportedOperationException

    override def ledgerIdentity(
        connection: Connection
    ): Option[ParameterStorageBackend.IdentityParams] =
      throw new UnsupportedOperationException

    override def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def updatePrunedAllDivulgedContractsUpToInclusive(prunedUpToInclusive: Offset)(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def prunedUptoInclusive(connection: Connection): Option[Offset] =
      throw new UnsupportedOperationException
  }
}

object SequentialWriteDaoSpec {

  private def offset(s: String): Offset = Offset.fromHexString(Ref.HexString.assertFromString(s))

  private def someUpdate(key: String) = Some(
    state.Update.PublicPackageUploadRejected(
      submissionId = Ref.SubmissionId.assertFromString("abc"),
      recordTime = Timestamp.assertFromInstant(Instant.now),
      rejectionReason = key,
    )
  )

  private val someParty = DbDto.PartyEntry(
    ledger_offset = "",
    recorded_at = null,
    submission_id = null,
    party = Some("party"),
    display_name = None,
    typ = "accept",
    rejection_reason = None,
    is_local = Some(true),
  )

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

  val singlePartyFixture: Option[state.Update.PublicPackageUploadRejected] =
    someUpdate("singleParty")
  val partyAndCreateFixture: Option[state.Update.PublicPackageUploadRejected] =
    someUpdate("partyAndCreate")
  val allEventsFixture: Option[state.Update.PublicPackageUploadRejected] =
    someUpdate("allEventsFixture")

  private val someUpdateToDbDtoFixture: Map[String, List[DbDto]] = Map(
    singlePartyFixture.get.rejectionReason -> List(someParty),
    partyAndCreateFixture.get.rejectionReason -> List(someParty, someEventCreated),
    allEventsFixture.get.rejectionReason -> List(
      someEventCreated,
      someEventExercise,
      someEventDivulgence,
    ),
  )

  val updateToDbDtoFixture: Offset => state.Update => Iterator[DbDto] =
    _ => {
      case r: state.Update.PublicPackageUploadRejected =>
        someUpdateToDbDtoFixture(r.rejectionReason).iterator
      case _ => throw new Exception
    }

  private val someConnection = mock[Connection]

}
