// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSource,
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterInput,
  PaginationInput,
}
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside}

private[backend] trait StorageBackendTestsInitializeIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initializeIngestion)"
  import StorageBackendTestValues.*

  private val signatory = Ref.Party.assertFromString("signatory")

  val dtos = Vector(
    // 1: party allocation
    dtoPartyEntry(offset(1), "party1")
  )
  it should "delete overspill entries - parties" in {
    fixture(
      dtos1 = dtos,
      lastOffset1 = 2L,
      lastEventSeqId1 = 0L,
      dtos2 = Vector(
        // 3: party allocation
        dtoPartyEntry(offset(3), "party2")
      ),
      lastOffset2 = 3L,
      lastEventSeqId2 = 0L,
      checkContentsBefore = () => {
        val parties = executeSql(backend.party.knownParties(None, 10))
        parties should have length 1
      },
      checkContentsAfter = () => {
        val parties = executeSql(backend.party.knownParties(None, 10))
        parties should have length 1
      },
    )
  }

  it should "delete overspill entries written before first ledger end update - parties" in {
    fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos = dtos,
      lastOffset = 3,
      lastEventSeqId = 0L,
      checkContentsAfter = () => {
        val parties2 = executeSql(backend.party.knownParties(None, 10))
        parties2 shouldBe empty
      },
    )
  }

  val dtos1 = Vector(
    // 1: transaction with a create node
    dtosCreate(
      1L,
      event_sequential_id = 1,
      internal_contract_id = 101,
      additional_witnesses = Set(someParty),
    )(stakeholders = Set(signatory, someParty)),
    Seq(
      dtoTransactionMeta(
        offset(1),
        event_sequential_id_first = 1L,
        event_sequential_id_last = 1L,
      ),
      dtoCompletion(offset(41)),
    ),
    // 2: transaction with exercise node
    dtosWitnessedExercised(
      2L,
      event_sequential_id = 2,
      consuming = false,
      internal_contract_id = Some(101),
      additional_witnesses = Set(someParty),
    ),
    dtosConsumingExercise(
      2L,
      event_sequential_id = 3,
      internal_contract_id = Some(102),
      stakeholders = Set(someParty),
      additional_witnesses = Set(someParty),
    ),
    Seq(
      dtoTransactionMeta(
        offset(2),
        event_sequential_id_first = 2L,
        event_sequential_id_last = 4L,
      ),
      dtoCompletion(offset(2)),
    ),
    // 3: assign
    dtosAssign(
      3L,
      event_sequential_id = 4,
      internal_contract_id = 103,
    )(stakeholders = Set(someParty)),
    // 4: unassign
    dtosUnassign(
      4L,
      event_sequential_id = 5,
      internal_contract_id = Some(103),
      stakeholders = Set(someParty),
    ),
    // 5: topology transactions
    Seq(
      dtoPartyToParticipant(
        offset(5),
        eventSequentialId = 6,
        party = someParty,
        participant = "someParticipant",
      ),
      dtoPartyToParticipant(
        offset(5),
        eventSequentialId = 7,
        party = someParty2,
        participant = "someParticipant",
      ),
    ),
  ).flatten

  it should "delete overspill entries - events, transaction meta, completions" in {
    val dtos2 = Vector(
      // 6: transaction with create node
      dtosCreate(
        6L,
        event_sequential_id = 8L,
        internal_contract_id = 201,
        additional_witnesses = Set(someParty),
      )(stakeholders = Set(signatory, someParty)),
      Seq(
        dtoTransactionMeta(
          offset(6),
          event_sequential_id_first = 8L,
          event_sequential_id_last = 8L,
        ),
        dtoCompletion(offset(6)),
      ),
      // 7: transaction with exercise node
      dtosWitnessedExercised(
        7L,
        event_sequential_id = 9L,
        consuming = false,
        internal_contract_id = Some(201),
        additional_witnesses = Set(someParty),
      ),
      dtosConsumingExercise(
        7L,
        event_sequential_id = 10L,
        internal_contract_id = Some(202),
        stakeholders = Set(someParty),
        additional_witnesses = Set(someParty),
      ),
      Seq(
        dtoTransactionMeta(
          offset(7),
          event_sequential_id_first = 9L,
          event_sequential_id_last = 10L,
        ),
        dtoCompletion(offset(7)),
      ),
      // 8: assign
      dtosAssign(8L, event_sequential_id = 11, internal_contract_id = 203)(stakeholders =
        Set(someParty)
      ),
      // 9: unassign
      dtosUnassign(
        9L,
        event_sequential_id = 12,
        internal_contract_id = Some(203),
        stakeholders = Set(someParty),
      ),
      // 10: topology transactions
      Seq(
        dtoPartyToParticipant(
          offset(10),
          eventSequentialId = 13,
          party = someParty,
          participant = "someParticipant",
        ),
        dtoPartyToParticipant(
          offset(10),
          eventSequentialId = 14,
          party = someParty3,
          participant = "someParticipant",
        ),
      ),
    ).flatten
    val allDtos = dtos1 ++ dtos2
    fixture(
      dtos1 = dtos1,
      lastOffset1 = 5L,
      lastEventSeqId1 = 7L,
      dtos2 = dtos2,
      lastOffset2 = 12L,
      lastEventSeqId2 = 15L,
      checkContentsBefore = () => {
        val activateEventSeqIds =
          executeSql(
            backend.event.fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Activate
            )(IdRange(1L, 100L), Some(Set.empty), None)
          ).map(_.eventSeqId)
        val deactivateEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Deactivate
          )(IdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val witnessEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(IdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(IdRange(1L, 100L))
          ).map(_.partyId)
        activateEventSeqIds shouldBe List(1, 4, 8, 11)
        deactivateEventSeqIds shouldBe List(3, 5, 10, 12)
        witnessEventSeqIds shouldBe List(2, 9)
        topologyPartyEvents shouldBe List(
          someParty,
          someParty2,
          someParty,
          someParty3,
        ) // not constrained by ledger end
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe Set((1, 1), (2, 4))
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe fetchIdsFromTransactionMetaOffsets(allDtos.collect {
          case meta: DbDto.TransactionMeta =>
            meta.event_offset
        })
        fetchIdsCreateStakeholder() shouldBe List(
          1L,
          8L,
        ) // since ledger-end does not limit the range query
        fetchIdsCreateNonStakeholder() shouldBe List(1L, 8L)
        fetchIdsConsumingStakeholder() shouldBe List(3L, 10L)
        fetchIdsConsumingNonStakeholder() shouldBe List(3L, 10L)
        fetchIdsNonConsuming() shouldBe List(2L, 9L)
        fetchIdsAssignStakeholder() shouldBe List(4L, 11L)
        fetchTopologyParty() shouldBe List(6, 13)
      },
      checkContentsAfter = () => {
        val activateEventSeqIds =
          executeSql(
            backend.event.fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Activate
            )(IdRange(1L, 100L), Some(Set.empty), None)
          ).map(_.eventSeqId)
        val deactivateEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Deactivate
          )(IdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val witnessEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(IdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(IdRange(1L, 100L))
          ).map(_.partyId)
        activateEventSeqIds shouldBe List(1, 4)
        deactivateEventSeqIds shouldBe List(3, 5)
        witnessEventSeqIds shouldBe List(2)
        topologyPartyEvents shouldBe List(
          someParty,
          someParty2,
        ) // not constrained by ledger end
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe Set((1, 1), (2, 4))
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe fetchIdsFromTransactionMetaOffsets(allDtos.collect {
          case meta: DbDto.TransactionMeta =>
            meta.event_offset
        })
        fetchIdsCreateStakeholder() shouldBe List(1L)
        fetchIdsCreateNonStakeholder() shouldBe List(1L)
        fetchIdsConsumingStakeholder() shouldBe List(3L)
        fetchIdsConsumingNonStakeholder() shouldBe List(3L)
        fetchIdsNonConsuming() shouldBe List(2L)
        fetchIdsAssignStakeholder() shouldBe List(4L)
        fetchTopologyParty() shouldBe List(6)
      },
    )
  }

  it should "delete overspill entries written before first ledger end update - events, transaction meta, completions" in {
    fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos = dtos1,
      lastOffset = 5,
      lastEventSeqId = 7L,
      checkContentsAfter = () => {
        val contractsCreated =
          executeSql(
            backend.contract
              .activeContracts(List(101, 201), 1000)
          )
        val contractsAssigned =
          executeSql(
            backend.contract
              .activeContracts(List(103, 203), 1000)
          )
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(IdRange(1L, 100L))
          ).map(_.partyId)
        contractsCreated should not contain hashCid("#101")
        contractsAssigned should not contain hashCid("#103")
        contractsAssigned should not contain hashCid("#203")
        topologyPartyEvents shouldBe empty
        fetchIdsFromTransactionMetaUpdateIds(dtos1.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe empty
        fetchIdsFromTransactionMetaOffsets(dtos1.collect { case meta: DbDto.TransactionMeta =>
          meta.event_offset
        }) shouldBe empty
        fetchIdsCreateStakeholder() shouldBe empty
        fetchIdsCreateNonStakeholder() shouldBe empty
        fetchIdsConsumingStakeholder() shouldBe empty
        fetchIdsConsumingNonStakeholder() shouldBe empty
        fetchIdsNonConsuming() shouldBe empty
        fetchIdsAssignStakeholder() shouldBe empty
        fetchTopologyParty() shouldBe empty
      },
    )
  }

  private def fetchIdsNonConsuming(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(
        EventIdSource.VariousWitnesses
      )(
        witnessO = Some(someParty),
        templateIdO = None,
        eventTypes = Set(PersistentEventType.NonConsumingExercise),
      )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchIdsConsumingNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.DeactivateWitnesses)(
          witnessO = Some(someParty),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.ConsumingExercise),
        )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchIdsConsumingStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.DeactivateStakeholder)(
          witnessO = Some(someParty),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.ConsumingExercise),
        )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchIdsCreateNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateWitnesses)(
          witnessO = Some(someParty),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Create),
        )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchIdsCreateStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(someParty),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Create),
        )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchIdsAssignStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(someParty),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Assign),
        )
    )(
      IdFilterInput(
        startExclusive = 0,
        endInclusive = 1000,
      )
    )

  private def fetchTopologyParty(): Vector[Long] =
    executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = Some(someParty)
      )(_)(
        PaginationInput(
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
      )
    )

  private def fetchIdsFromTransactionMetaUpdateIds(
      updateIds: Seq[Array[Byte]]
  ): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.updatePointwiseQueries
    updateIds
      .map(UpdateId.tryFromByteArray)
      .map { updateId =>
        executeSql(
          txPointwiseQueries.fetchIdsFromUpdateMeta(
            lookupKey = LookupKey.ByUpdateId(updateId)
          )
        )
      }
      .flatMap(_.toList)
      .toSet
  }

  private def fetchIdsFromTransactionMetaOffsets(offsets: Seq[Long]): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.updatePointwiseQueries
    offsets
      .map(Offset.tryFromLong)
      .map { offset =>
        executeSql(
          txPointwiseQueries.fetchIdsFromUpdateMeta(
            lookupKey = LookupKey.ByOffset(offset)
          )
        )
      }
      .flatMap(_.toList)
      .toSet
  }

  private def fixture(
      dtos1: Vector[DbDto],
      lastOffset1: Long,
      lastEventSeqId1: Long,
      dtos2: Vector[DbDto],
      lastOffset2: Long,
      lastEventSeqId2: Long,
      checkContentsBefore: () => Assertion,
      checkContentsAfter: () => Assertion,
  ): Assertion = {
    val loggerFactory = SuppressingLogger(getClass)
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))
    // Fully insert first batch of updates
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset1, lastEventSeqId1)))
    // Partially insert second batch of updates (indexer crashes before updating ledger end)
    executeSql(ingest(dtos2, _))
    // Check the contents
    checkContentsBefore()
    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))
    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset2 + 1, lastEventSeqId2 + 1)))
    // Check the contents
    checkContentsAfter()
  }

  private def fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos: Vector[DbDto],
      lastOffset: Long,
      lastEventSeqId: Long,
      checkContentsAfter: () => Assertion,
  ): Assertion = {
    val loggerFactory = SuppressingLogger(getClass)
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))
    // Insert first batch of updates, but crash before writing the first ledger end
    executeSql(ingest(dtos, _))
    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))
    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset + 1, lastEventSeqId + 1)))
    checkContentsAfter()
  }
}
