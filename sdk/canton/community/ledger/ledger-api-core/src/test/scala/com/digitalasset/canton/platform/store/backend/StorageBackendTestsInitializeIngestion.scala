// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.TransactionMetering
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.common.EventIdSource
import com.digitalasset.canton.platform.store.backend.common.TransactionPointwiseQueries.LookupKey
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.Inside
import org.scalatest.compatible.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitializeIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initializeIngestion)"

  import StorageBackendTestValues.*

  private def dtoMetering(app: String, offset: Offset) =
    dtoTransactionMetering(
      TransactionMetering(
        userId = Ref.UserId.assertFromString(app),
        actionCount = 1,
        meteringTimestamp = someTime,
        ledgerOffset = offset,
      )
    )

  private val signatory = Ref.Party.assertFromString("signatory")

  {
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
  }

  {
    val dtos = Vector(
      dtoMetering("AppA", offset(1)),
      dtoMetering("AppB", offset(4)),
    )
    it should "delete overspill entries - metering" in {
      fixture(
        dtos1 = dtos,
        lastOffset1 = 4L,
        lastEventSeqId1 = 0L,
        dtos2 = Vector(
          dtoMetering("AppC", offset(6))
        ),
        lastOffset2 = 6L,
        lastEventSeqId2 = 0L,
        checkContentsBefore = () => {
          val metering =
            executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))
          // Metering report can include partially ingested data in non-final reports
          metering.applicationData should have size 3
          metering.isFinal shouldBe false
        },
        checkContentsAfter = () => {
          val metering =
            executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))
          metering.applicationData should have size 2 // Partially ingested data removed
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - metering" in {
      fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 4,
        lastEventSeqId = 0L,
        checkContentsAfter = () => {
          val metering2 =
            executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))
          metering2.applicationData shouldBe empty
        },
      )
    }
  }

  {
    val dtos = Vector(
      // 1: transaction with a create node
      dtoCreate(offset(1), 1L, hashCid("#101"), signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, someParty),
      DbDto.IdFilterCreateNonStakeholderInformee(1L, someTemplateId.toString, someParty),
      dtoTransactionMeta(
        offset(1),
        event_sequential_id_first = 1L,
        event_sequential_id_last = 1L,
      ),
      dtoCompletion(offset(41)),
      // 2: transaction with exercise node
      dtoExercise(offset(2), 2L, false, hashCid("#101")),
      DbDto.IdFilterNonConsumingInformee(2L, someTemplateId.toString, someParty),
      dtoExercise(offset(2), 3L, true, hashCid("#102")),
      DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, someParty),
      DbDto.IdFilterConsumingNonStakeholderInformee(3L, someTemplateId.toString, someParty),
      dtoTransactionMeta(
        offset(2),
        event_sequential_id_first = 2L,
        event_sequential_id_last = 4L,
      ),
      dtoCompletion(offset(2)),
      // 3: assign
      dtoAssign(
        offset(3),
        eventSequentialId = 4,
        contractId = hashCid("#103"),
      ),
      DbDto.IdFilterAssignStakeholder(4, someTemplateId.toString, someParty),
      DbDto.IdFilterAssignStakeholder(4, someTemplateId.toString, someParty2),
      // 4: unassign
      dtoUnassign(
        offset(4),
        eventSequentialId = 5,
        contractId = hashCid("#103"),
      ),
      DbDto.IdFilterUnassignStakeholder(5, someTemplateId.toString, someParty),
      DbDto.IdFilterUnassignStakeholder(5, someTemplateId.toString, someParty2),
      // 5: topology transactions
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
    )

    it should "delete overspill entries - events, transaction meta, completions" in {
      val dtos2 = Vector(
        // 6: transaction with create node
        dtoCreate(offset(6), 8L, hashCid("#201"), signatory = signatory),
        DbDto.IdFilterCreateStakeholder(8L, someTemplateId.toString, someParty),
        DbDto.IdFilterCreateNonStakeholderInformee(8L, someTemplateId.toString, someParty),
        dtoTransactionMeta(
          offset(6),
          event_sequential_id_first = 8L,
          event_sequential_id_last = 8L,
        ),
        dtoCompletion(offset(6)),
        // 7: transaction with exercise node
        dtoExercise(offset(7), 9L, false, hashCid("#201")),
        DbDto.IdFilterNonConsumingInformee(9L, someTemplateId.toString, someParty),
        dtoExercise(offset(7), 10L, true, hashCid("#202")),
        DbDto.IdFilterConsumingStakeholder(10L, someTemplateId.toString, someParty),
        DbDto.IdFilterConsumingNonStakeholderInformee(10L, someTemplateId.toString, someParty),
        dtoTransactionMeta(
          offset(7),
          event_sequential_id_first = 9L,
          event_sequential_id_last = 10L,
        ),
        dtoCompletion(offset(7)),
        // 8: assign
        dtoAssign(
          offset(8),
          eventSequentialId = 11,
          contractId = hashCid("#203"),
        ),
        DbDto.IdFilterAssignStakeholder(11, someTemplateId.toString, someParty),
        DbDto.IdFilterAssignStakeholder(11, someTemplateId.toString, someParty2),
        // 9: unassign
        dtoUnassign(
          offset(9),
          eventSequentialId = 12,
          contractId = hashCid("#203"),
        ),
        DbDto.IdFilterUnassignStakeholder(12, someTemplateId.toString, someParty),
        DbDto.IdFilterUnassignStakeholder(12, someTemplateId.toString, someParty2),
        // 10: topology transactions
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
      )
      val allDtos = dtos ++ dtos2
      fixture(
        dtos1 = dtos,
        lastOffset1 = 5L,
        lastEventSeqId1 = 7L,
        dtos2 = dtos2,
        lastOffset2 = 12L,
        lastEventSeqId2 = 15L,
        checkContentsBefore = () => {
          val contractsCreated =
            executeSql(
              backend.contract
                .createdContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          val contractsArchived =
            executeSql(
              backend.contract
                .archivedContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          val contractsAssigned =
            executeSql(
              backend.contract
                .assignedContracts(List(hashCid("#103"), hashCid("#203")), offset(1000))
            )
          val assignedEvents =
            executeSql(
              backend.event.assignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.rawCreatedEvent.contractId)
          val unassignedEvents =
            executeSql(
              backend.event.unassignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.contractId)
          val topologyPartyEvents =
            executeSql(
              backend.event.topologyPartyEventBatch(1L to 100L)
            ).map(_.partyId)
          contractsCreated.get(hashCid("#101")) should not be empty
          contractsCreated.get(hashCid("#201")) should not be empty
          contractsArchived.get(hashCid("#101")) shouldBe empty
          contractsArchived.get(hashCid("#201")) shouldBe empty
          contractsAssigned.get(hashCid("#103")) should not be empty
          contractsAssigned.get(hashCid("#203")) should not be empty
          assignedEvents shouldBe List(
            hashCid("#103"),
            hashCid("#203"),
          ) // not constrained by ledger end
          unassignedEvents shouldBe List(
            hashCid("#103"),
            hashCid("#203"),
          ) // not constrained by ledger end
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
          fetchIdsUnassignStakeholder() shouldBe List(5L, 12L)
          fetchTopologyParty() shouldBe List(6, 13)
        },
        checkContentsAfter = () => {
          val contractsCreated =
            executeSql(
              backend.contract
                .createdContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          val contractsArchived =
            executeSql(
              backend.contract
                .archivedContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          val contractsAssigned =
            executeSql(
              backend.contract
                .assignedContracts(List(hashCid("#103"), hashCid("#203")), offset(1000))
            )
          val assignedEvents =
            executeSql(
              backend.event.assignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.rawCreatedEvent.contractId)
          val unassignedEvents =
            executeSql(
              backend.event.unassignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.contractId)
          val topologyPartyEvents =
            executeSql(
              backend.event.topologyPartyEventBatch(1L to 100L)
            ).map(_.partyId)
          contractsCreated.get(hashCid("#101")) should not be empty
          contractsCreated.get(hashCid("#201")) shouldBe empty
          contractsArchived.get(hashCid("#101")) shouldBe empty
          contractsArchived.get(hashCid("#201")) shouldBe empty
          contractsAssigned.get(hashCid("#103")) should not be empty
          contractsAssigned.get(hashCid("#203")) shouldBe empty
          assignedEvents shouldBe List(hashCid("#103")) // not constrained by ledger end
          unassignedEvents shouldBe List(hashCid("#103")) // not constrained by ledger end
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
          fetchIdsUnassignStakeholder() shouldBe List(5L)
          fetchTopologyParty() shouldBe List(6)
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - events, transaction meta, completions" in {
      fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 5,
        lastEventSeqId = 7L,
        checkContentsAfter = () => {
          val contractsCreated =
            executeSql(
              backend.contract
                .createdContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          val contractsAssigned =
            executeSql(
              backend.contract
                .assignedContracts(List(hashCid("#103"), hashCid("#203")), offset(1000))
            )
          val assignedEvents =
            executeSql(
              backend.event.assignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.rawCreatedEvent.contractId)
          val unassignedEvents =
            executeSql(
              backend.event.unassignEventBatch(1L to 100L, Some(Set.empty))
            ).map(_.event.contractId)
          val topologyPartyEvents =
            executeSql(
              backend.event.topologyPartyEventBatch(1L to 100L)
            ).map(_.partyId)
          contractsCreated.get(hashCid("#101")) shouldBe None
          contractsAssigned.get(hashCid("#103")) shouldBe empty
          contractsAssigned.get(hashCid("#203")) shouldBe empty
          assignedEvents shouldBe empty
          unassignedEvents shouldBe empty
          topologyPartyEvents shouldBe empty
          fetchIdsFromTransactionMetaUpdateIds(dtos.collect { case meta: DbDto.TransactionMeta =>
            meta.update_id
          }) shouldBe empty
          fetchIdsFromTransactionMetaOffsets(dtos.collect { case meta: DbDto.TransactionMeta =>
            meta.event_offset
          }) shouldBe empty
          fetchIdsCreateStakeholder() shouldBe empty
          fetchIdsCreateNonStakeholder() shouldBe empty
          fetchIdsConsumingStakeholder() shouldBe empty
          fetchIdsConsumingNonStakeholder() shouldBe empty
          fetchIdsNonConsuming() shouldBe empty
          fetchIdsAssignStakeholder() shouldBe empty
          fetchIdsUnassignStakeholder() shouldBe empty
          fetchTopologyParty() shouldBe empty
        },
      )
    }
  }

  private def fetchIdsNonConsuming(): Vector[Long] =
    executeSql(
      backend.event.transactionStreamingQueries.fetchEventIds(
        EventIdSource.NonConsumingInformee
      )(
        stakeholderO = Some(someParty),
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

  private def fetchIdsConsumingNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIds(EventIdSource.ConsumingNonStakeholder)(
          stakeholderO = Some(someParty),
          templateIdO = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )

  private def fetchIdsConsumingStakeholder(): Vector[Long] =
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIds(EventIdSource.ConsumingStakeholder)(
          stakeholderO = Some(someParty),
          templateIdO = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )

  private def fetchIdsCreateNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIds(EventIdSource.CreateNonStakeholder)(
          stakeholderO = Some(someParty),
          templateIdO = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )

  private def fetchIdsCreateStakeholder(): Vector[Long] =
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIds(EventIdSource.CreateStakeholder)(
          stakeholderO = Some(someParty),
          templateIdO = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )

  private def fetchIdsAssignStakeholder(): Vector[Long] =
    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

  private def fetchIdsUnassignStakeholder(): Vector[Long] =
    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

  private def fetchTopologyParty(): Vector[Long] =
    executeSql(
      backend.event.fetchTopologyPartyEventIds(
        party = Some(someParty),
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

  private def fetchIdsFromTransactionMetaUpdateIds(udpateIds: Seq[String]): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.transactionPointwiseQueries
    udpateIds
      .map(Ref.TransactionId.assertFromString)
      .map { updateId =>
        executeSql(txPointwiseQueries.fetchIdsFromTransactionMeta(LookupKey.UpdateId(updateId)))
      }
      .flatMap(_.toList)
      .toSet
  }

  private def fetchIdsFromTransactionMetaOffsets(offsets: Seq[Long]): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.transactionPointwiseQueries
    offsets
      .map(Offset.tryFromLong)
      .map { offset =>
        executeSql(txPointwiseQueries.fetchIdsFromTransactionMeta(LookupKey.Offset(offset)))
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
    executeSql(
      backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd, loggerFactory)
    )
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
    executeSql(
      backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd, loggerFactory)
    )
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
