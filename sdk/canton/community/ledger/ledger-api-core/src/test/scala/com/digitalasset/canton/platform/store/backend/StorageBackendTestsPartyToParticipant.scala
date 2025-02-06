// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.Revoked
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawParticipantAuthorization
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsPartyToParticipant
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend (party to participant)"

  import StorageBackendTestValues.*

  val otherParticipantId: ParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("participant")
  )

  private val singleDto = Vector(
    dtoPartyToParticipant(offset(1), 1L)
  )

  private val multipleDtos = Vector(
    dtoPartyToParticipant(offset(1), 1L),
    dtoPartyToParticipant(offset(2), 2L, someParty2),
    dtoPartyToParticipant(offset(3), 3L, someParty, otherParticipantId.toString),
    dtoPartyToParticipant(offset(4), 4L, someParty, someParticipantId.toString, Revoked),
  )

  def toRaw(dbDto: DbDto.EventPartyToParticipant): RawParticipantAuthorization =
    RawParticipantAuthorization(
      offset = Offset.tryFromLong(dbDto.event_offset),
      updateId = dbDto.update_id,
      partyId = dbDto.party_id,
      participantId = dbDto.participant_id,
      participant_permission =
        EventStorageBackend.intToAuthorizationLevel(dbDto.participant_permission),
      recordTime = Timestamp.assertFromLong(dbDto.record_time),
      synchronizerId = dbDto.synchronizer_id,
      traceContext = Some(dbDto.trace_context),
    )

  private def sanitize: RawParticipantAuthorization => RawParticipantAuthorization =
    _.copy(traceContext = None)

  it should "return correct index for a single party to participant mapping" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(singleDto, _))
    val eventsForAll = executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    executeSql(
      updateLedgerEnd(offset(1), ledgerEndSequentialId = 1L)
    )
    val eventsForSomeParty = executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = Some(someParty),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    eventsForAll should not be empty
    eventsForSomeParty should not be empty
  }

  it should "return correct indices for multiple party to participant mappings" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    val eventsForAll = executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    executeSql(
      updateLedgerEnd(offset(4), ledgerEndSequentialId = 4L)
    )
    val eventsForSomeParty = executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = Some(someParty),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    eventsForAll should contain theSameElementsAs Vector(1L, 2L, 3L, 4L)
    eventsForSomeParty should contain theSameElementsAs Vector(1L, 3L, 4L)
  }

  it should "respond with payloads for a single party to participant mapping" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(singleDto, _))
    val payloadsForAll = executeSql(
      backend.event.topologyPartyEventBatch(Vector(1L))
    )

    payloadsForAll should not be empty
    payloadsForAll.map(sanitize) should contain theSameElementsAs singleDto.map(toRaw).map(sanitize)
  }

  it should "respond with payloads for a multiple party to participant mappings" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    val payloadsForAll = executeSql(
      backend.event.topologyPartyEventBatch(Vector(1L, 2L, 3L, 4L))
    )

    payloadsForAll should not be empty
    payloadsForAll
      .map(sanitize) should contain theSameElementsAs multipleDtos.map(toRaw).map(sanitize)
  }

  behavior of "topologyEventPublishedOnRecordTime"

  private val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")

  it should "be true if there is one" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          dtoPartyToParticipant(
            offset(1),
            1L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1504),
          ),
          dtoPartyToParticipant(
            offset(2),
            2L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1505),
          ),
          dtoPartyToParticipant(
            offset(3),
            3L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1506),
          ),
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    backend.stringInterningSupport.synchronizerId.internalize(synchronizerId1)
    backend.stringInterningSupport.synchronizerId.internalize(synchronizerId2)
    executeSql(
      backend.event
        .topologyEventPublishedOnRecordTime(synchronizerId1, CantonTimestamp.ofEpochMicro(1505))
    ) shouldBe true
  }

  it should "be false if there is none" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          dtoPartyToParticipant(
            offset(1),
            1L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1504),
          ),
          dtoPartyToParticipant(
            offset(3),
            3L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1506),
          ),
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    executeSql(
      backend.event
        .topologyEventPublishedOnRecordTime(synchronizerId1, CantonTimestamp.ofEpochMilli(1505))
    ) shouldBe false
  }

  it should "be false if it is on a different synchronizer" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          dtoPartyToParticipant(
            offset(1),
            1L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1504),
          ),
          dtoPartyToParticipant(
            offset(2),
            2L,
            synchronizerId = synchronizerId2.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1505),
          ),
          dtoPartyToParticipant(
            offset(3),
            3L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1506),
          ),
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    executeSql(
      backend.event
        .topologyEventPublishedOnRecordTime(synchronizerId1, CantonTimestamp.ofEpochMilli(1505))
    ) shouldBe false
  }

  it should "be false if it is after the ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          dtoPartyToParticipant(
            offset(1),
            1L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1504),
          ),
          dtoPartyToParticipant(
            offset(2),
            2L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1505),
          ),
          dtoPartyToParticipant(
            offset(3),
            3L,
            synchronizerId = synchronizerId1.toProtoPrimitive,
            recordTime = Timestamp.assertFromLong(1506),
          ),
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    executeSql(
      backend.event
        .topologyEventPublishedOnRecordTime(synchronizerId1, CantonTimestamp.ofEpochMilli(1505))
    ) shouldBe false
  }
}
