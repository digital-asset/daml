// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.{
  EventIdSourceForInformees,
  EventPayloadSourceForTreeTx,
}
import com.daml.platform.store.dao.events.Raw
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsPruning
    extends Matchers
    with OptionValues
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (pruning)"

  import StorageBackendTestValues._

  it should "correctly update the pruning offset" in {
    val offset_1 = offset(3)
    val offset_2 = offset(2)
    val offset_3 = offset(4)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    val initialPruningOffset = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_1))
    val updatedPruningOffset_1 = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_2))
    val updatedPruningOffset_2 = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_3))
    val updatedPruningOffset_3 = executeSql(backend.parameter.prunedUpToInclusive)

    initialPruningOffset shouldBe empty
    updatedPruningOffset_1 shouldBe Some(offset_1)
    // The pruning offset is not updated if lower than the existing offset
    updatedPruningOffset_2 shouldBe Some(offset_1)
    updatedPruningOffset_3 shouldBe Some(offset_3)
  }

  it should "correctly update the pruning offset of all divulged contracts" in {
    val offset_1 = offset(3)
    val offset_2 = offset(2)
    val offset_3 = offset(4)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    val initialPruningOffset = executeSql(
      backend.parameter.participantAllDivulgedContractsPrunedUpToInclusive
    )

    executeSql(
      backend.parameter.updatePrunedAllDivulgedContractsUpToInclusive(offset_1)
    )
    val updatedPruningOffset_1 = executeSql(
      backend.parameter.participantAllDivulgedContractsPrunedUpToInclusive
    )

    executeSql(
      backend.parameter.updatePrunedAllDivulgedContractsUpToInclusive(offset_2)
    )
    val updatedPruningOffset_2 = executeSql(
      backend.parameter.participantAllDivulgedContractsPrunedUpToInclusive
    )

    executeSql(
      backend.parameter.updatePrunedAllDivulgedContractsUpToInclusive(offset_3)
    )
    val updatedPruningOffset_3 = executeSql(
      backend.parameter.participantAllDivulgedContractsPrunedUpToInclusive
    )

    initialPruningOffset shouldBe empty
    updatedPruningOffset_1 shouldBe Some(offset_1)
    // The pruning offset is not updated if lower than the existing offset
    updatedPruningOffset_2 shouldBe Some(offset_1)
    updatedPruningOffset_3 shouldBe Some(offset_3)
  }

  // TODO etq: Test pruning consuming and non-consuming events
  it should "prune consuming and non-consuming events" in {
    val signatoryParty = Ref.Party.assertFromString("signatory")
    val actorParty = Ref.Party.assertFromString("actor")
    val nonConsuming = dtoExercise(
      offset = offset(3),
      eventSequentialId = 5L,
      contractId = hashCid("#1"),
      consuming = false,
      signatory = signatoryParty,
    )
    val nonConsumingFilter1 = DbDto.IdFilterNonConsumingInformee(5L, signatoryParty)
    val consuming = dtoExercise(
      offset = offset(4),
      eventSequentialId = 6L,
      contractId = hashCid("#1"),
      consuming = true,
      signatory = signatoryParty,
      actor = actorParty,
    )
    val consumingFilter1 =
      DbDto.IdFilterConsumingStakeholder(6L, someTemplateId.toString, signatoryParty)
    val consumingFilter2 = DbDto.IdFilterConsumingNonStakeholderInformee(6L, actorParty)
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          nonConsuming,
          nonConsumingFilter1,
          consuming,
          consumingFilter1,
          consumingFilter2,
        ),
        _,
      )
    )
    val endOffset = offset(4)
    val endId = 6L
    executeSql(updateLedgerEnd(endOffset, endId))

    def fetchIdsNonConsuming_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.NonConsumingInformee
          )(informee = signatoryParty, startExclusive = 0, endInclusive = endId, limit = 10)
      )
    }

    def fetchIdsConsumingStakeholder_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.ConsumingStakeholder
          )(informee = signatoryParty, startExclusive = 0, endInclusive = endId, limit = 10)
      )
    }
    def fetchIdsConsumingNonStakeholder_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.ConsumingNonStakeholder
          )(informee = actorParty, startExclusive = 0, endInclusive = endId, limit = 10)
      )
    }

    def fetchTreeNonConsuming_streaming(
        eventSequentialIds: Iterable[Long]
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
          target = EventPayloadSourceForTreeTx.NonConsuming
        )(
          eventSequentialIds = eventSequentialIds,
          allFilterParties = Set(signatoryParty),
        )
      )
    }

    def fetchTreeConsuming_streaming(
        eventSequentialIds: Iterable[Long]
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
          target = EventPayloadSourceForTreeTx.Consuming
        )(
          eventSequentialIds = eventSequentialIds,
          allFilterParties = Set(signatoryParty, actorParty),
        )
      )
    }
    // before pruning
    val before1_idsNonConsuming = fetchIdsNonConsuming_streaming
    val before2_idsConsumingStakeholder = fetchIdsConsumingStakeholder_streaming
    val before3_idsConsumingNonStakeholder = fetchIdsConsumingNonStakeholder_streaming
    val before4_eventsTreeNonConsuming = fetchTreeNonConsuming_streaming(before1_idsNonConsuming)
    val before5_eventsTreeConsuming = fetchTreeConsuming_streaming(
      before2_idsConsumingStakeholder ++ before3_idsConsumingNonStakeholder
    )
    before1_idsNonConsuming should not be empty
    before2_idsConsumingStakeholder should not be empty
    before3_idsConsumingNonStakeholder should not be empty
    before4_eventsTreeNonConsuming should not be empty
    before5_eventsTreeConsuming should not be empty
    // Prune before the offset at which we ingested our events
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    val interim1_idsNonConsuming = fetchIdsNonConsuming_streaming
    val interim2_idsConsumingStakeholder = fetchIdsConsumingStakeholder_streaming
    val interim3_idsConsumingNonStakeholder = fetchIdsConsumingNonStakeholder_streaming
    val interim4_eventsTreeNonConsuming = fetchTreeNonConsuming_streaming(before1_idsNonConsuming)
    val interim5_eventsTreeConsuming = fetchTreeConsuming_streaming(
      before2_idsConsumingStakeholder ++ before3_idsConsumingNonStakeholder
    )
    interim1_idsNonConsuming should not be empty
    interim2_idsConsumingStakeholder should not be empty
    interim3_idsConsumingNonStakeholder should not be empty
    interim4_eventsTreeNonConsuming should not be empty
    interim5_eventsTreeConsuming should not be empty
    // Prune at the ledger end
    executeSql(
      backend.event.pruneEvents(endOffset, pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(endOffset))
    // after pruning
    val after1_idsNonConsuming = fetchIdsNonConsuming_streaming
    val after2_idsConsumingStakeholder = fetchIdsConsumingStakeholder_streaming
    val after3_idsConsumingNonStakeholder = fetchIdsConsumingNonStakeholder_streaming
    val after4_eventsTreeNonConsuming = fetchTreeNonConsuming_streaming(before1_idsNonConsuming)
    val after5_eventsTreeConsuming = fetchTreeConsuming_streaming(
      before2_idsConsumingStakeholder ++ before3_idsConsumingNonStakeholder
    )
    after1_idsNonConsuming shouldBe empty
    after2_idsConsumingStakeholder shouldBe empty
    after3_idsConsumingNonStakeholder shouldBe empty
    after4_eventsTreeNonConsuming shouldBe empty
    after5_eventsTreeConsuming shouldBe empty
  }

  it should "prune an archived contract" in {
    val signatoryParty = Ref.Party.assertFromString("signatory")
    val observerParty = Ref.Party.assertFromString("observer")
    val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
    // a create event in its own transaction
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    val createFilter1 = DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatoryParty)
    val createFilter2 = DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, observerParty)
    val createFilter3 = DbDto.IdFilterCreateNonStakeholderInformee(1L, nonStakeholderInformeeParty)
    val createTxId = dtoTransactionId(create)
    val createTxMeta = DbDto.TransactionMeta(
      transaction_id = createTxId,
      event_offset = create.event_offset.get,
      event_sequential_id_first = create.event_sequential_id,
      event_sequential_id_last = create.event_sequential_id,
    )
    // a consuming event in its own transaction
    val archive = dtoExercise(
      offset = offset(2),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
    )
    val archiveFilter1 =
      DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty)
    val archiveFilter2 =
      DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, observerParty)
    val archiveTxMeta = DbDto.TransactionMeta(
      transaction_id = dtoTransactionId(archive),
      event_offset = archive.event_offset.get,
      event_sequential_id_first = archive.event_sequential_id,
      event_sequential_id_last = archive.event_sequential_id,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          create,
          createFilter1,
          createFilter2,
          createFilter3,
          createTxMeta,
          archive,
          archiveFilter1,
          archiveFilter2,
          archiveTxMeta,
        ),
        _,
      )
    )
    executeSql(updateLedgerEnd(offset(2), 2L))

    def fetchIdsSignatory_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateStakeholder
          )(informee = signatoryParty, startExclusive = 0, endInclusive = 2L, limit = 10)
      )
    }

    def fetchIdsObserver_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateStakeholder
          )(informee = observerParty, startExclusive = 0, endInclusive = 2L, limit = 10)
      )
    }

    def fetchIdsNonStakeholders_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateNonStakeholder
          )(
            informee = nonStakeholderInformeeParty,
            startExclusive = 0,
            endInclusive = 2L,
            limit = 10,
          )
      )
    }

    def fetchTreeEvents_streaming(
        eventSequentialIds: Iterable[Long]
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
          target = EventPayloadSourceForTreeTx.Create
        )(
          eventSequentialIds = eventSequentialIds,
          allFilterParties = Set(signatoryParty),
        )
      )
    }

    def fetchEventsTree_pointwise(
        firstEventSequentialId: Long,
        lastEventSequentialId: Long,
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionPointwiseQueries.fetchTreeTransactionEvents(
          firstEventSequentialId = firstEventSequentialId,
          lastEventSequentialId = lastEventSequentialId,
          requestingParties = Set(signatoryParty, observerParty, nonStakeholderInformeeParty),
        )
      )
    }

    // Make sure the entries related to the create event are visible
    val before1_idsSignatory_streaming = fetchIdsSignatory_streaming
    val before2_idsObserver_streaming = fetchIdsObserver_streaming
    val before3_idsNonStakeholders_streaming = fetchIdsNonStakeholders_streaming
    val before4_treeEvents_streaming = fetchTreeEvents_streaming(
      before1_idsSignatory_streaming ++ before2_idsObserver_streaming ++ before3_idsNonStakeholders_streaming
    )
    val before5_ids_pointwise =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val before6_eventsTree_pointwise =
      fetchEventsTree_pointwise(before5_ids_pointwise.value._1, before5_ids_pointwise.value._2)
    val before7_txMeta =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val before8_rawEvents = executeSql(backend.event.rawEvents(0, 2L))
    val before9_activeContracts = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString(signatoryParty)), 2L)
    )
    before1_idsSignatory_streaming should not be empty
    before2_idsObserver_streaming should not be empty
    before3_idsNonStakeholders_streaming should not be empty
    before4_treeEvents_streaming should not be empty
    before5_ids_pointwise should not be empty
    before6_eventsTree_pointwise should not be empty
    before7_txMeta should not be empty
    before8_rawEvents should not be empty
    before9_activeContracts shouldBe empty
    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    // Make sure the entries related to the create event are not visible anymore
    val after1_idsSignatory = fetchIdsSignatory_streaming
    val after2_idsObserver = fetchIdsObserver_streaming
    val after3_idsNonStakeholders = executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIdsForInformee(
          target = EventIdSourceForInformees.CreateNonStakeholder
        )(informee = signatoryParty, startExclusive = 0, endInclusive = 2L, limit = 10)
    )
    val after4_treeEvents = fetchTreeEvents_streaming(
      before1_idsSignatory_streaming ++ before2_idsObserver_streaming ++ before3_idsNonStakeholders_streaming
    )
    val after5_ids_pointwise =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val after6_eventsTree_pointwise =
      fetchEventsTree_pointwise(before5_ids_pointwise.value._1, before5_ids_pointwise.value._2)
    val after7_txMeta =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val after8_rawEvents = executeSql(backend.event.rawEvents(0, 2L))
    val after9_activeContracts = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString(signatoryParty)), 2L)
    )
    after1_idsSignatory shouldBe empty
    after2_idsObserver shouldBe empty
    after3_idsNonStakeholders shouldBe empty
    after4_treeEvents shouldBe empty
    after5_ids_pointwise shouldBe empty
    after6_eventsTree_pointwise shouldBe empty
    after7_txMeta shouldBe empty
    after8_rawEvents shouldBe empty
    after9_activeContracts shouldBe empty
  }

  it should "not prune an active contract" in {
    val signatoryParty = Ref.Party.assertFromString("signatory")
    val observerParty = Ref.Party.assertFromString("observer")
    val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
    val partyEntry = dtoPartyEntry(offset(1), signatoryParty)
    val create = dtoCreate(
      offset = offset(2),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    val createFilter1 = DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatoryParty)
    val createFilter2 = DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, observerParty)
    val createFilter3 = DbDto.IdFilterCreateNonStakeholderInformee(1L, nonStakeholderInformeeParty)
    val createTxId = dtoTransactionId(create)
    val createTxMeta = DbDto.TransactionMeta(
      transaction_id = createTxId,
      event_offset = create.event_offset.get,
      event_sequential_id_first = create.event_sequential_id,
      event_sequential_id_last = create.event_sequential_id,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          partyEntry,
          create,
          createFilter1,
          createFilter2,
          createFilter3,
          createTxMeta,
        ),
        _,
      )
    )
    executeSql(updateLedgerEnd(offset(2), 1L))

    def fetchIdsSignatory_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateStakeholder
          )(informee = signatoryParty, startExclusive = 0, endInclusive = 1L, limit = 10)
      )
    }

    def fetchIdsObserver_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateStakeholder
          )(informee = observerParty, startExclusive = 0, endInclusive = 1L, limit = 10)
      )
    }

    def fetchIdsNonStakeholders_streaming: Seq[Long] = {
      executeSql(
        backend.event.transactionStreamingQueries
          .fetchEventIdsForInformee(
            target = EventIdSourceForInformees.CreateNonStakeholder
          )(
            informee = nonStakeholderInformeeParty,
            startExclusive = 0,
            endInclusive = 1L,
            limit = 10,
          )
      )
    }

    def fetchTreeEvents_streaming(
        eventSequentialIds: Iterable[Long]
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
          target = EventPayloadSourceForTreeTx.Create
        )(
          eventSequentialIds = eventSequentialIds,
          allFilterParties = Set(signatoryParty),
        )
      )
    }

    def fetchEventsTree_pointwise(
        firstEventSequentialId: Long,
        lastEventSequentialId: Long,
    ): Seq[EventStorageBackend.Entry[Raw.TreeEvent]] = {
      executeSql(
        backend.event.transactionPointwiseQueries.fetchTreeTransactionEvents(
          firstEventSequentialId = firstEventSequentialId,
          lastEventSequentialId = lastEventSequentialId,
          requestingParties = Set(signatoryParty, observerParty, nonStakeholderInformeeParty),
        )
      )
    }

    // Make sure the entries relate to the create event are visible
    val before_idsSignatory_streaming = fetchIdsSignatory_streaming
    val before_idsObserver_streaming = fetchIdsObserver_streaming
    val before_idsNonStakeholders_streaming = fetchIdsNonStakeholders_streaming
    val before_treeEvents_streaming = fetchTreeEvents_streaming(
      before_idsSignatory_streaming ++ before_idsObserver_streaming ++ before_idsNonStakeholders_streaming
    )
    val before_ids_pointwise =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val before_eventsTree_pointwise =
      fetchEventsTree_pointwise(before_ids_pointwise.value._1, before_ids_pointwise.value._2)
    val before_txMeta =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val before_rawEvents = executeSql(backend.event.rawEvents(0, 1L))
    val before_activeContracts = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString(signatoryParty)), 1L)
    )
    before_idsSignatory_streaming should not be empty
    before_idsNonStakeholders_streaming should not be empty
    before_treeEvents_streaming should not be empty
    before_ids_pointwise should not be empty
    before_eventsTree_pointwise should not be empty
    before_txMeta should not be empty
    before_rawEvents should not be empty
    before_activeContracts should have size 1
    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))

    // Make sure the entries related to the create event are still visible - active contracts should not be pruned
    val after_idsStakeholders_streaming = fetchIdsSignatory_streaming
    val after_idsNonStakeholders_streaming = fetchIdsNonStakeholders_streaming
    val after_treeEvents_streaming = fetchTreeEvents_streaming(
      before_idsSignatory_streaming ++ before_idsObserver_streaming ++ before_idsNonStakeholders_streaming
    )
    val after_ids_pointwise =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val after_eventsTree_pointwise =
      fetchEventsTree_pointwise(before_ids_pointwise.value._1, before_ids_pointwise.value._2)
    val after_txMeta =
      executeSql(backend.event.transactionPointwiseQueries.fetchIdsFromTransactionMeta(createTxId))
    val after_rawEvents = executeSql(backend.event.rawEvents(0, 1L))
    val after_activeContracts = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString(signatoryParty)), 1L)
    )
    // Note: while the ACS service should still see active contracts, the transaction stream service should not return
    // any data before the last pruning offset. For pointwise transaction lookups, we check the pruning offset
    // inside the database query - that's why they do not return any results. For transaction stream lookups, we only
    // check the pruning offset when starting the stream - that's why those queries still return data here.
    after_idsStakeholders_streaming should not be empty
    after_idsNonStakeholders_streaming should not be empty
    after_treeEvents_streaming should not be empty
    after_ids_pointwise shouldBe empty
    after_eventsTree_pointwise shouldBe empty
    after_txMeta shouldBe empty
    after_rawEvents should not be empty
    after_activeContracts should have size 1
  }

  it should "prune all retroactively and immediately divulged contracts (if pruneAllDivulgedContracts is set)" in {
    val partyName = "party"
    val divulgee = Ref.Party.assertFromString(partyName)
    val contract1_id = hashCid("#1")
    val contract2_id = hashCid("#2")

    // This contract is an example of the immediate divulgence
    // because its signatory party is not hosted on the participant
    // at the contract's offset.
    val contract1_immediateDivulgence = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = divulgee,
    )
    val partyEntry = dtoPartyEntry(offset(2), partyName)
    val contract2_createWithLocalStakeholder = dtoCreate(
      offset = offset(3),
      eventSequentialId = 2L,
      contractId = contract2_id,
      signatory = divulgee,
    )
    val contract1_retroactiveDivulgence =
      dtoDivulgence(
        offset = Some(offset(3)),
        eventSequentialId = 3L,
        contractId = contract1_id,
        divulgee = partyName,
      )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_immediateDivulgence,
          partyEntry,
          contract1_retroactiveDivulgence,
          contract2_createWithLocalStakeholder,
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val contract1_beforePruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract1_id,
      )
    )
    val contract2_beforePruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract2_id,
      )
    )
    executeSql(
      backend.event.pruneEvents(offset(3), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    val contract1_afterPruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract1_id,
      )
    )
    val contract2_afterPruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract2_id,
      )
    )

    contract1_beforePruning should not be empty
    contract2_beforePruning should not be empty

    contract1_afterPruning shouldBe empty
    // Immediate divulgence for contract2 occurred after the associated party became locally hosted
    // so it is not pruned
    contract2_afterPruning should not be empty
  }

  it should "only prune retroactively divulged contracts if there exists an associated consuming exercise (if pruneAllDivulgedContracts is not set)" in {
    val signatory = "signatory"
    val divulgee = Ref.Party.assertFromString("party")
    val contract1_id = hashCid("#1")
    val contract2_id = hashCid("#2")

    val contract1_create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = signatory,
    )
    val contract1_divulgence = dtoDivulgence(
      offset = Some(offset(2)),
      eventSequentialId = 2L,
      contractId = contract1_id,
      divulgee = "party",
    )
    val contract1_consumingExercise = dtoExercise(
      offset = offset(3),
      eventSequentialId = 3L,
      consuming = true,
      contractId = contract1_id,
    )
    val contract2_divulgence = dtoDivulgence(
      offset = Some(offset(4)),
      eventSequentialId = 4L,
      contractId = contract2_id,
      divulgee = divulgee,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_create,
          contract1_divulgence,
          contract1_consumingExercise,
          contract2_divulgence,
        ),
        _,
      )
    )
    // Set the ledger end past the last ingested event so we can prune up to it inclusively
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )
    val contract1_beforePruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract1_id,
      )
    )
    val contract2_beforePruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract2_id,
      )
    )
    executeSql(
      backend.event.pruneEvents(offset(4), pruneAllDivulgedContracts = false)(
        _,
        loggingContext,
      )
    )
    val contract1_afterPruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract1_id,
      )
    )
    val contract2_afterPruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        contract2_id,
      )
    )

    // Contract 1 appears as active tu `divulgee` before pruning
    contract1_beforePruning should not be empty
    // Contract 2 appears as active tu `divulgee` before pruning
    contract2_beforePruning should not be empty

    // Contract 1 should not be visible anymore to `divulgee` after pruning
    contract1_afterPruning shouldBe empty
    // Contract 2 did not have a locally stored exercise event
    // hence its divulgence is not pruned - appears active to `divulgee`
    contract2_afterPruning should not be empty
  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitter = someParty,
    )
    val applicationId = dtoApplicationId(completion)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest a completion
    executeSql(ingest(Vector(completion), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    // Make sure the completion is visible
    val before = executeSql(
      backend.completion.commandCompletions(
        offset(0),
        offset(1),
        applicationId,
        Set(someParty),
        limit = 10,
      )
    )

    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, loggingContext))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(1)))

    // Make sure the completion is not visible anymore
    val after = executeSql(
      backend.completion.commandCompletions(
        offset(0),
        offset(1),
        applicationId,
        Set(someParty),
        limit = 10,
      )
    )

    before should not be empty
    after shouldBe empty
  }
}
