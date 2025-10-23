// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{SynchronizerIndex, Update}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{
  DbDto,
  IngestionStorageBackend,
  ParameterStorageBackend,
}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.SequentialWriteDaoSpec.*
import com.digitalasset.canton.platform.store.interning.{
  DomainStringIterators,
  InternizingStringInterningView,
  StringInterning,
  StringInterningDomain,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{NameTypeConRef, PackageId, Party, UserId}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import scala.concurrent.blocking

class SequentialWriteDaoSpec extends AnyFlatSpec with Matchers {

  behavior of "SequentialWriteDaoImpl"

  it should "store correctly in a happy path case" in {
    val storageBackendCaptor =
      new StorageBackendCaptor(Some(LedgerEnd(offset(1), 5, 1, CantonTimestamp.MinValue)))
    val ledgerEndCache = MutableLedgerEndCache()
    val testee = SequentialWriteDaoImpl(
      parameterStorageBackend = storageBackendCaptor,
      ingestionStorageBackend = storageBackendCaptor,
      updateToDbDtos = updateToDbDtoFixture,
      ledgerEndCache = ledgerEndCache,
      stringInterningView = stringInterningViewFixture,
      dbDtosToStringsForInterning = dbDtoToStringsForInterningFixture,
    )
    testee.store(someConnection, offset(2L), singlePartyFixture)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(2L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(5L)
    testee.store(someConnection, offset(3L), allEventsFixture)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(3L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(7L)
    testee.store(someConnection, offset(4L), None)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(4L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(7L)
    testee.store(someConnection, offset(5L), partyAndCreateFixture)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(5L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(8L)

    storageBackendCaptor.captured(0) shouldBe someParty
    storageBackendCaptor
      .captured(1) shouldBe LedgerEnd(
      offset(2L),
      5,
      1,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor
      .captured(2)
      .asInstanceOf[DbDto.EventActivate]
      .event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(3)
      .asInstanceOf[DbDto.IdFilterActivateStakeholder]
      .idFilter
      .event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(4)
      .asInstanceOf[DbDto.IdFilterActivateStakeholder]
      .idFilter
      .event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(5)
      .asInstanceOf[DbDto.EventDeactivate]
      .event_sequential_id shouldBe 7
    storageBackendCaptor
      .captured(6) shouldBe LedgerEnd(
      offset(3L),
      7,
      1,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor
      .captured(7) shouldBe LedgerEnd(
      offset(4L),
      7,
      1,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor.captured(8) shouldBe someParty
    storageBackendCaptor
      .captured(9)
      .asInstanceOf[DbDto.EventActivate]
      .event_sequential_id shouldBe 8
    storageBackendCaptor
      .captured(10) shouldBe LedgerEnd(
      offset(5L),
      8,
      1,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor.captured should have size 11
  }

  it should "start event_seq_id from 1" in {
    val storageBackendCaptor = new StorageBackendCaptor(LedgerEnd.beforeBegin)
    val ledgerEndCache = MutableLedgerEndCache()
    val testee = SequentialWriteDaoImpl(
      parameterStorageBackend = storageBackendCaptor,
      ingestionStorageBackend = storageBackendCaptor,
      updateToDbDtos = updateToDbDtoFixture,
      ledgerEndCache = ledgerEndCache,
      stringInterningView = stringInterningViewFixture,
      dbDtosToStringsForInterning = dbDtoToStringsForInterningFixture,
    )
    testee.store(someConnection, offset(3L), None)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(3L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(0L)
    testee.store(someConnection, offset(4L), partyAndCreateFixture)
    ledgerEndCache().map(_.lastOffset) shouldBe Some(offset(4L))
    ledgerEndCache().map(_.lastEventSeqId) shouldBe Some(1L)

    storageBackendCaptor
      .captured(0) shouldBe LedgerEnd(
      offset(3L),
      0,
      0,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor.captured(1) shouldBe someParty
    storageBackendCaptor
      .captured(2)
      .asInstanceOf[DbDto.EventActivate]
      .event_sequential_id shouldBe 1
    storageBackendCaptor
      .captured(3) shouldBe LedgerEnd(
      offset(4L),
      1,
      0,
      CantonTimestamp.MinValue,
    )
    storageBackendCaptor.captured should have size 4
  }

  class StorageBackendCaptor(initialLedgerEnd: Option[ParameterStorageBackend.LedgerEnd])
      extends IngestionStorageBackend[Vector[DbDto]]
      with ParameterStorageBackend {

    var captured: Vector[Any] = Vector.empty

    override def batch(dbDtos: Vector[DbDto], stringInterning: StringInterning): Vector[DbDto] =
      dbDtos

    override def insertBatch(connection: Connection, batch: Vector[DbDto]): Unit = blocking(
      synchronized {
        connection shouldBe someConnection
        captured = captured ++ batch
      }
    )

    override def deletePartiallyIngestedData(ledgerEnd: Option[ParameterStorageBackend.LedgerEnd])(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def updateLedgerEnd(
        params: ParameterStorageBackend.LedgerEnd,
        synchronizerIndexes: Map[SynchronizerId, SynchronizerIndex],
    )(connection: Connection): Unit =
      blocking(synchronized {
        connection shouldBe someConnection
        captured = captured :+ params
      })

    private var ledgerEndCalled = false
    override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
      blocking(synchronized {
        connection shouldBe someConnection
        ledgerEndCalled shouldBe false
        ledgerEndCalled = true
        initialLedgerEnd
      })

    override def initializeParameters(
        params: ParameterStorageBackend.IdentityParams,
        loggerFactory: NamedLoggerFactory,
    )(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def ledgerIdentity(
        connection: Connection
    ): Option[ParameterStorageBackend.IdentityParams] =
      throw new UnsupportedOperationException

    override def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def prunedUpToInclusive(connection: Connection): Option[Offset] =
      throw new UnsupportedOperationException

    override def prunedUpToInclusiveAndLedgerEnd(
        connection: Connection
    ): ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd =
      throw new UnsupportedOperationException

    override def cleanSynchronizerIndex(synchronizerId: SynchronizerId)(
        connection: Connection
    ): Option[SynchronizerIndex] =
      throw new UnsupportedOperationException

    override def updatePostProcessingEnd(postProcessingEnd: Option[Offset])(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def postProcessingEnd(connection: Connection): Option[Offset] =
      throw new UnsupportedOperationException
  }
}

object SequentialWriteDaoSpec {

  private val serializableTraceContext =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

  private val externalTransactionHash =
    Hash
      .digest(HashPurpose.PreparedSubmission, ByteString.copyFromUtf8("mock_hash"), Sha256)
      .unwrap
      .toByteArray

  private def offset(l: Long): Offset = Offset.tryFromLong(l)

  private def hashCid(key: String): ContractId =
    ContractId.V1(com.digitalasset.daml.lf.crypto.Hash.hashPrivateKey(key))

  private def someUpdate(key: String) = Some(
    Update.PartyAddedToParticipant(
      party = Ref.Party.assertFromString(key),
      participantId = Ref.ParticipantId.assertFromString("participant"),
      recordTime = CantonTimestamp.now(),
      submissionId = Some(Ref.SubmissionId.assertFromString("abc")),
    )(TraceContext.empty)
  )

  private val someParty = DbDto.PartyEntry(
    ledger_offset = 1,
    recorded_at = 0,
    submission_id = null,
    party = Some("party"),
    typ = "accept",
    rejection_reason = None,
    is_local = Some(true),
  )

  private val someEventActivate = DbDto.EventActivate(
    event_offset = 1,
    update_id = new Array[Byte](0),
    command_id = None,
    workflow_id = None,
    submitters = None,
    node_id = 3,
    representative_package_id = "3",
    create_key_hash = None,
    event_sequential_id = 0,
    synchronizer_id = SynchronizerId.tryFromString("x::synchronizer"),
    trace_context = serializableTraceContext,
    record_time = 0,
    external_transaction_hash = Some(externalTransactionHash),
    internal_contract_id = 42L,
    event_type = 1,
    additional_witnesses = Some(Set.empty),
    source_synchronizer_id = None,
    reassignment_id = None,
    reassignment_counter = None,
    notPersistedContractId = hashCid("24"),
  )

  private val someEventDeactivate = DbDto.EventDeactivate(
    event_offset = 1,
    update_id = new Array[Byte](0),
    ledger_effective_time = None,
    command_id = None,
    workflow_id = None,
    submitters = None,
    node_id = 3,
    contract_id = hashCid("24"),
    template_id = "",
    package_id = "2",
    exercise_choice = Some(""),
    exercise_choice_interface_id = None,
    exercise_argument = Some(Array.empty),
    exercise_result = None,
    exercise_actors = Some(Set.empty),
    exercise_last_descendant_node_id = Some(3),
    exercise_argument_compression = None,
    exercise_result_compression = None,
    event_sequential_id = 0,
    synchronizer_id = SynchronizerId.tryFromString("x::synchronizer"),
    trace_context = serializableTraceContext,
    record_time = 0,
    external_transaction_hash = Some(externalTransactionHash),
    deactivated_event_sequential_id = None,
    event_type = 3,
    additional_witnesses = None,
    internal_contract_id = None,
    stakeholders = Set.empty,
    reassignment_id = None,
    reassignment_counter = None,
    assignment_exclusivity = None,
    target_synchronizer_id = None,
  )

  val singlePartyFixture: Option[Update.PartyAddedToParticipant] =
    someUpdate("singleParty")
  val partyAndCreateFixture: Option[Update.PartyAddedToParticipant] =
    someUpdate("partyAndCreate")
  val allEventsFixture: Option[Update.PartyAddedToParticipant] =
    someUpdate("allEventsFixture")

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private val someUpdateToDbDtoFixture: Map[Ref.Party, List[DbDto]] = Map(
    singlePartyFixture.get.party -> List(someParty),
    partyAndCreateFixture.get.party -> List(someParty, someEventActivate),
    allEventsFixture.get.party -> List(
      someEventActivate,
      DbDto.IdFilter(0L, "", "", first_per_sequential_id = true).activateStakeholder,
      DbDto.IdFilter(0L, "", "", first_per_sequential_id = false).activateStakeholder,
      someEventDeactivate,
    ),
  )

  private val updateToDbDtoFixture: Offset => Update => Iterator[DbDto] =
    _ => {
      case r: Update.PartyAddedToParticipant =>
        someUpdateToDbDtoFixture(r.party).iterator
      case _ => throw new Exception
    }

  private val dbDtoToStringsForInterningFixture: Iterable[DbDto] => DomainStringIterators = {
    case iterable if iterable.sizeIs == 5 =>
      new DomainStringIterators(
        parties = Iterator.empty,
        templateIds = List("1").iterator,
        synchronizerIds = Iterator.empty,
        packageIds = Iterator("2"),
        userIds = Iterator.empty,
        participantIds = Iterator.empty,
        choiceNames = Iterator.empty,
        interfaceIds = Iterator.empty,
      )
    case _ =>
      new DomainStringIterators(
        parties = Iterator.empty,
        templateIds = Iterator.empty,
        synchronizerIds = Iterator.empty,
        packageIds = Iterator.empty,
        userIds = Iterator.empty,
        participantIds = Iterator.empty,
        choiceNames = Iterator.empty,
        interfaceIds = Iterator.empty,
      )
  }

  private val stringInterningViewFixture: StringInterning with InternizingStringInterningView =
    new StringInterning with InternizingStringInterningView {
      override def templateId: StringInterningDomain[NameTypeConRef] =
        ???

      override def packageId: StringInterningDomain[PackageId] =
        ???

      override def party: StringInterningDomain[Party] = ???

      override def synchronizerId: StringInterningDomain[SynchronizerId] =
        ???

      override def userId: StringInterningDomain[UserId] = ???

      override def participantId: StringInterningDomain[Ref.ParticipantId] =
        ???

      override def choiceName: StringInterningDomain[Ref.ChoiceName] =
        ???

      override def interfaceId: StringInterningDomain[Ref.Identifier] =
        ???

      override def internize(
          domainStringIterators: DomainStringIterators
      ): Iterable[(Int, String)] =
        if (domainStringIterators.templateIds.isEmpty) Nil
        else List(1 -> "a", 2 -> "b")

    }

  private val someConnection = mock[Connection]

}
