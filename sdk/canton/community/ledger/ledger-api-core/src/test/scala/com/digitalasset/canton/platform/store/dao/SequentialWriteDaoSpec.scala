// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, Update}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.PackageName
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.sql.Connection
import scala.concurrent.blocking

class SequentialWriteDaoSpec extends AnyFlatSpec with Matchers {

  import TraceContext.Implicits.Empty.*
  implicit val TracedConverter: Option[Update] => Option[Traced[Update]] = _.map(Traced[Update])
  behavior of "SequentialWriteDaoImpl"

  it should "store correctly in a happy path case" in {
    val storageBackendCaptor =
      new StorageBackendCaptor(LedgerEnd(Offset.beforeBegin, 5, 1, CantonTimestamp.MinValue))
    val ledgerEndCache = MutableLedgerEndCache()
    val testee = SequentialWriteDaoImpl(
      parameterStorageBackend = storageBackendCaptor,
      ingestionStorageBackend = storageBackendCaptor,
      updateToDbDtos = updateToDbDtoFixture,
      ledgerEndCache = ledgerEndCache,
      stringInterningView = stringInterningViewFixture,
      dbDtosToStringsForInterning = dbDtoToStringsForInterningFixture,
    )
    testee.store(someConnection, offset("01"), singlePartyFixture)
    ledgerEndCache() shouldBe (offset("01") -> 5)
    testee.store(someConnection, offset("02"), allEventsFixture)
    ledgerEndCache() shouldBe (offset("02") -> 7)
    testee.store(someConnection, offset("03"), None)
    ledgerEndCache() shouldBe (offset("03") -> 7)
    testee.store(someConnection, offset("04"), partyAndCreateFixture)
    ledgerEndCache() shouldBe (offset("04") -> 8)

    storageBackendCaptor.captured(0) shouldBe someParty
    storageBackendCaptor
      .captured(1) shouldBe LedgerEnd(offset("01"), 5, 1, CantonTimestamp.MinValue)
    storageBackendCaptor.captured(2).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(3)
      .asInstanceOf[DbDto.IdFilterCreateStakeholder]
      .event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(4)
      .asInstanceOf[DbDto.IdFilterCreateStakeholder]
      .event_sequential_id shouldBe 6
    storageBackendCaptor
      .captured(5)
      .asInstanceOf[DbDto.EventExercise]
      .event_sequential_id shouldBe 7
    storageBackendCaptor
      .captured(6) shouldBe LedgerEnd(offset("02"), 7, 1, CantonTimestamp.MinValue)
    storageBackendCaptor
      .captured(7) shouldBe LedgerEnd(offset("03"), 7, 1, CantonTimestamp.MinValue)
    storageBackendCaptor.captured(8) shouldBe someParty
    storageBackendCaptor.captured(9).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 8
    storageBackendCaptor
      .captured(10) shouldBe LedgerEnd(offset("04"), 8, 1, CantonTimestamp.MinValue)
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
    testee.store(someConnection, offset("03"), None)
    ledgerEndCache() shouldBe (offset("03") -> 0)
    testee.store(someConnection, offset("04"), partyAndCreateFixture)
    ledgerEndCache() shouldBe (offset("04") -> 1)

    storageBackendCaptor
      .captured(0) shouldBe LedgerEnd(offset("03"), 0, 0, CantonTimestamp.MinValue)
    storageBackendCaptor.captured(1) shouldBe someParty
    storageBackendCaptor.captured(2).asInstanceOf[DbDto.EventCreate].event_sequential_id shouldBe 1
    storageBackendCaptor
      .captured(3) shouldBe LedgerEnd(offset("04"), 1, 0, CantonTimestamp.MinValue)
    storageBackendCaptor.captured should have size 4
  }

  class StorageBackendCaptor(initialLedgerEnd: ParameterStorageBackend.LedgerEnd)
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

    override def deletePartiallyIngestedData(ledgerEnd: ParameterStorageBackend.LedgerEnd)(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def updateLedgerEnd(
        params: ParameterStorageBackend.LedgerEnd,
        domainIndexes: Map[DomainId, DomainIndex],
    )(connection: Connection): Unit =
      blocking(synchronized {
        connection shouldBe someConnection
        captured = captured :+ params
      })

    private var ledgerEndCalled = false
    override def ledgerEnd(connection: Connection): ParameterStorageBackend.LedgerEnd =
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

    override def updatePrunedAllDivulgedContractsUpToInclusive(prunedUpToInclusive: Offset)(
        connection: Connection
    ): Unit =
      throw new UnsupportedOperationException

    override def prunedUpToInclusive(connection: Connection): Option[Offset] =
      throw new UnsupportedOperationException

    override def participantAllDivulgedContractsPrunedUpToInclusive(
        connection: Connection
    ): Option[Offset] =
      throw new UnsupportedOperationException

    override def prunedUpToInclusiveAndLedgerEnd(
        connection: Connection
    ): ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd =
      throw new UnsupportedOperationException

    override def domainLedgerEnd(domainId: DomainId)(connection: Connection): DomainIndex =
      throw new UnsupportedOperationException

    override def updatePostProcessingEnd(postProcessingEnd: Offset)(connection: Connection): Unit =
      throw new UnsupportedOperationException

    override def postProcessingEnd(connection: Connection): Option[Offset] =
      throw new UnsupportedOperationException
  }
}

object SequentialWriteDaoSpec {

  private val serializableTraceContext =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

  private def offset(s: String): Offset = Offset.fromHexString(Ref.HexString.assertFromString(s))

  private def someUpdate(key: String) = Some(
    Update.PartyAllocationRejected(
      submissionId = Ref.SubmissionId.assertFromString("abc"),
      participantId = Ref.ParticipantId.assertFromString("participant"),
      recordTime = Timestamp.now(),
      rejectionReason = key,
    )
  )

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

  private val someEventCreated = DbDto.EventCreate(
    event_offset = "",
    update_id = "",
    ledger_effective_time = 3,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = 3,
    event_id = "",
    contract_id = "1",
    template_id = "",
    package_name = "2",
    package_version = Some("1.2"),
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
    domain_id = "x::domain",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  private val someEventExercise = DbDto.EventExercise(
    consuming = true,
    event_offset = "",
    transaction_id = "",
    ledger_effective_time = 3,
    command_id = None,
    workflow_id = None,
    application_id = None,
    submitters = None,
    node_index = 3,
    event_id = "",
    contract_id = "1",
    template_id = "",
    package_name = "2",
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
    domain_id = "x::domain",
    trace_context = serializableTraceContext,
    record_time = 0,
  )

  val singlePartyFixture: Option[Update.PartyAllocationRejected] =
    someUpdate("singleParty")
  val partyAndCreateFixture: Option[Update.PartyAllocationRejected] =
    someUpdate("partyAndCreate")
  val allEventsFixture: Option[Update.PartyAllocationRejected] =
    someUpdate("allEventsFixture")

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private val someUpdateToDbDtoFixture: Map[String, List[DbDto]] = Map(
    singlePartyFixture.get.rejectionReason -> List(someParty),
    partyAndCreateFixture.get.rejectionReason -> List(someParty, someEventCreated),
    allEventsFixture.get.rejectionReason -> List(
      someEventCreated,
      DbDto.IdFilterCreateStakeholder(0L, "", ""),
      DbDto.IdFilterCreateStakeholder(0L, "", ""),
      someEventExercise,
    ),
  )

  private val updateToDbDtoFixture: Offset => Traced[Update] => Iterator[DbDto] =
    _ => {
      case Traced(r: Update.PartyAllocationRejected) =>
        someUpdateToDbDtoFixture(r.rejectionReason).iterator
      case _ => throw new Exception
    }

  private val dbDtoToStringsForInterningFixture: Iterable[DbDto] => DomainStringIterators = {
    case iterable if iterable.size == 5 =>
      new DomainStringIterators(
        Iterator.empty,
        List("1").iterator,
        Iterator.empty,
        Iterator("2"),
        Iterator("1.2"),
      )
    case _ =>
      new DomainStringIterators(
        Iterator.empty,
        Iterator.empty,
        Iterator.empty,
        Iterator.empty,
        Iterator.empty,
      )
  }

  private val stringInterningViewFixture: StringInterning with InternizingStringInterningView =
    new StringInterning with InternizingStringInterningView {
      override def templateId: StringInterningDomain[Ref.Identifier] =
        throw new NotImplementedException

      override def packageName: StringInterningDomain[PackageName] =
        throw new NotImplementedException

      override def party: StringInterningDomain[Party] = throw new NotImplementedException

      override def domainId: StringInterningDomain[DomainId] = throw new NotImplementedException

      override def packageVersion: StringInterningDomain[Ref.PackageVersion] =
        throw new NotImplementedException
      override def internize(
          domainStringIterators: DomainStringIterators
      ): Iterable[(Int, String)] =
        if (domainStringIterators.templateIds.isEmpty) Nil
        else List(1 -> "a", 2 -> "b")

    }

  private val someConnection = mock[Connection]

}
