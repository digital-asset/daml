// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.TransactionId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsPruning extends Matchers with StorageBackendSpec {
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

  it should "prune an archived contract" in {
    val someParty = Ref.Party.assertFromString("party")
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = someParty,
    )
    val createFilter1 = DbDto.CreateFilter_Stakeholder(1L, someTemplateId.toString, someParty)
    val createFilter2 = DbDto.CreateFilter_Stakeholder(1L, someTemplateId.toString, "observer")
    val createTransactionId: TransactionId = dtoTransactionId(create)
    val archive = dtoExercise(
      offset = offset(2),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = someParty,
    )
    val consumingFilter1 = DbDto.ConsumingFilter_Stakeholder(1L, someTemplateId.toString, someParty)
    val consumingFilter2 =
      DbDto.ConsumingFilter_Stakeholder(1L, someTemplateId.toString, "observer")
    val transactionMeta1 = DbDto.TransactionMeta(
      transaction_id = createTransactionId,
      event_offset = create.event_offset.get,
      event_sequential_id_from = create.event_sequential_id,
      event_sequential_id_to = create.event_sequential_id,
    )
    val transactionMeta2 = DbDto.TransactionMeta(
      transaction_id = dtoTransactionId(archive),
      event_offset = archive.event_offset.get,
      event_sequential_id_from = archive.event_sequential_id,
      event_sequential_id_to = archive.event_sequential_id,
    )
//    val range = RangeParams(0L, 2L, None, None)
    val requestingParties = Set(someParty)
//    val filter = FilterParams(Set(someParty), Set.empty)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          create,
          createFilter1,
          createFilter2,
          transactionMeta1,
          archive,
          consumingFilter1,
          consumingFilter2,
          transactionMeta2,
        ),
        _,
      )
    )
    executeSql(updateLedgerEnd(offset(2), 2L))

    // Make sure the events are visible
    val before1_ids = executeSql(
      backend.event.fetchIds_create_stakeholders(
        partyFilter = someParty,
        templateIdFilter = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 2L,
        limit = 10,
      )
    )
    val before1 = executeSql(
      backend.event
        .fetchEventPayloadsFlat(
          target = PayloadFetchingForFlatTxTarget.CreateEventPayloads
        )(eventSequentialIds = before1_ids, allFilterParties = Set(someParty))
    )
    val before3a: Option[(Long, Long)] =
      executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val before3 = executeSql(
      backend.event.fetchFlatTransaction(
        firstEventSequentialId = before3a.get._1,
        lastEventSequentialId = before3a.get._2,
        requestingParties = requestingParties,
      )
    )
    val before4_ids = executeSql(
      backend.event.fetchEventIdsForInformees(
        target = EventIdFetchingForInformeesTarget.CreateNonStakeholderInformee
      )(
        informee = someParty,
        startExclusive = 0,
        endInclusive = 2L,
        limit = 10,
      )
    )
    val before4 = executeSql(
      backend.event.fetchEventPayloadsFlat(
        target = PayloadFetchingForFlatTxTarget.CreateEventPayloads
      )(
        eventSequentialIds = before1_ids ++ before4_ids,
        allFilterParties = Set(someParty),
      )
    )
    val before5a = executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val before5 = executeSql(
      backend.event.fetchTreeTransaction(
        firstEventSequentialId = before5a.get._1,
        lastEventSequentialId = before5a.get._2,
        requestingParties = requestingParties,
      )
    )
    val before6 = executeSql(backend.event.rawEvents(0, 2L))
    val before7 = executeSql(
      backend.event
        .fetchIds_create_stakeholders(Ref.Party.assertFromString(someParty), None, 0L, 2L, 1000)
    )
    val before8 = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString(someParty)), 2L)
    )

    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))

    // Make sure the events are not visible anymore
    val after1_ids = executeSql(
      backend.event.fetchIds_create_stakeholders(
        partyFilter = someParty,
        templateIdFilter = None,
        startExclusive = 0,
        endInclusive = 2L,
        limit = 10,
      )
    )
    val after1 = executeSql(
      backend.event
        .fetchEventPayloadsFlat(
          target = PayloadFetchingForFlatTxTarget.CreateEventPayloads
        )(eventSequentialIds = before1_ids, allFilterParties = Set(someParty))
    )
    val after3a: Option[(Long, Long)] =
      executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val after3 = executeSql(
      backend.event.fetchFlatTransaction(
        firstEventSequentialId = before3a.get._1,
        lastEventSequentialId = before3a.get._2,
        requestingParties = requestingParties,
      )
    )
    val after4_ids = executeSql(
      backend.event.fetchEventIdsForInformees(
        target = EventIdFetchingForInformeesTarget.CreateNonStakeholderInformee
      )(
        informee = someParty,
        startExclusive = 0,
        endInclusive = 2L,
        limit = 10,
      )
    )
    val after4 = executeSql(
      backend.event.fetchEventPayloadsFlat(
        target = PayloadFetchingForFlatTxTarget.CreateEventPayloads
      )(
        eventSequentialIds = before1_ids ++ before4_ids,
        allFilterParties = Set(someParty),
      )
    )
    val after5a = executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val after5 = executeSql(
      backend.event.fetchTreeTransaction(
        firstEventSequentialId = before5a.get._1,
        lastEventSequentialId = before5a.get._2,
        requestingParties = requestingParties,
      )
    )

    val after6 = executeSql(backend.event.rawEvents(0, 2L))
    val after7 = executeSql(
      backend.event
        .fetchIds_create_stakeholders(Ref.Party.assertFromString("signatory"), None, 0L, 2L, 1000)
    )
    val after8 = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString("signatory")), 2L)
    )

    before1_ids should not be empty
    before1 should not be empty
    before3a should not be empty
    before3 should not be empty
    before4 should not be empty
    before5a should not be empty
    before5 should not be empty
    before6 should not be empty
    before7 should have size 1
    before8 shouldBe empty

    after1_ids shouldBe empty
    after1 shouldBe empty
    after3a shouldBe empty
    after3 shouldBe empty
    after4_ids shouldBe empty
    after4 shouldBe empty
    after5a shouldBe empty
    after5 shouldBe empty
    after6 shouldBe empty
    after7 shouldBe empty
    after8 shouldBe empty
  }

  it should "not prune an active contract" in {
    val partyName = "party"
    val someParty = Ref.Party.assertFromString(partyName)
    val partyEntry = dtoPartyEntry(offset(1), "party")
    val create = dtoCreate(
      offset = offset(2),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = someParty,
    )
    val createFilter1 = DbDto.CreateFilter_Stakeholder(1L, someTemplateId.toString, "signatory")
    val createFilter2 = DbDto.CreateFilter_Stakeholder(1L, someTemplateId.toString, "observer")
    val createTransactionId = dtoTransactionId(create)

    val transactionMeta = DbDto.TransactionMeta(
      transaction_id = createTransactionId,
      event_offset = create.event_offset.get,
      event_sequential_id_from = create.event_sequential_id,
      event_sequential_id_to = create.event_sequential_id,
    )

//    val range = RangeParams(0L, 1L, None, None)
//    val filter = FilterParams(Set(someParty), Set.empty)
    val requestingParties = Set(someParty)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          partyEntry,
          create,
          createFilter1,
          createFilter2,
          transactionMeta,
        ),
        _,
      )
    )
    executeSql(updateLedgerEnd(offset(2), 1L))

    // Make sure the events are visible
//    val before1 = executeSql(backend.event.transactionEvents(range, filter))
    val before3a: Option[(Long, Long)] =
      executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val before3 = executeSql(
      backend.event.fetchFlatTransaction(
        firstEventSequentialId = before3a.get._1,
        lastEventSequentialId = before3a.get._2,
        requestingParties = requestingParties,
      )
    )

//    val before4 = executeSql(backend.event.transactionTreeEvents(range, filter))

    val before5a = executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val before5 = executeSql(
      backend.event.fetchTreeTransaction(
        firstEventSequentialId = before5a.get._1,
        lastEventSequentialId = before5a.get._2,
        requestingParties = requestingParties,
      )
    )
    val before6 = executeSql(backend.event.rawEvents(0, 1L))
    val before7 = executeSql(
      backend.event
        .fetchIds_create_stakeholders(Ref.Party.assertFromString("signatory"), None, 0L, 1L, 1000)
    )
    val before8 = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString("signatory")), 1L)
    )

    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))

    // Make sure the events are still visible - active contracts should not be pruned
//    val after1 = executeSql(backend.event.transactionEvents(range, filter))
    val after3a: Option[(Long, Long)] =
      executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val after3 = executeSql(
      backend.event.fetchFlatTransaction(
        firstEventSequentialId = before3a.get._1,
        lastEventSequentialId = before3a.get._2,
        requestingParties = requestingParties,
      )
    )
//    val after4 = executeSql(backend.event.transactionTreeEvents(range, filter))
    val after5a = executeSql(backend.event.fetchIdsFromTransactionMeta(createTransactionId))
    val after5 = executeSql(
      backend.event.fetchTreeTransaction(
        firstEventSequentialId = before5a.get._1,
        lastEventSequentialId = before5a.get._2,
        requestingParties = requestingParties,
      )
    )
    val after6 = executeSql(backend.event.rawEvents(0, 1L))
    val after7 = executeSql(
      backend.event
        .fetchIds_create_stakeholders(Ref.Party.assertFromString("signatory"), None, 0L, 1L, 1000)
    )
    val after8 = executeSql(
      backend.event
        .activeContractEventBatch(List(1L), Set(Ref.Party.assertFromString("signatory")), 1L)
    )

//    before1 should not be empty
    before3a should not be empty
    before3 should not be empty
//    before4 should not be empty
    before5a should not be empty
    before5 should not be empty
    before6 should not be empty
    before7 should have size 1
    before8 should have size 1

    // Note: while the ACS service should still see active contracts, the transaction stream service should not return
    // any data before the last pruning offset. For pointwise transaction lookups, we check the pruning offset
    // inside the database query - that's why they do not return any results. For transaction stream lookups, we only
    // check the pruning offset when starting the stream - that's why those queries still return data here.
//    after1 should not be empty // transaction stream query
    after3a shouldBe empty // pointwise transaction lookup
    after3 shouldBe empty // pointwise transaction lookup
//    after4 should not be empty // transaction stream query
    after5a shouldBe empty // pointwise transaction lookup
    after5 shouldBe empty // pointwise transaction lookup
    after6 should not be empty
    after7 should have size 1
    after8 should have size 1
  }

  it should "prune all retroactively and immediately divulged contracts (if pruneAllDivulgedContracts is set)" in {
    val partyName = "party"
    val divulgee = Ref.Party.assertFromString(partyName)
    val contract1_id = hashCid("#1")
    val contract2_id = hashCid("#2")

    // TODO pbatko: How does this represent immediate divulgence?
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
    val transactionMeta1 = DbDto.TransactionMeta(
      transaction_id = contract1_immediateDivulgence.transaction_id.get,
      event_offset = contract1_immediateDivulgence.event_offset.get,
      event_sequential_id_from = contract1_immediateDivulgence.event_sequential_id,
      event_sequential_id_to = contract1_immediateDivulgence.event_sequential_id,
    )
    val transactionMeta2 = DbDto.TransactionMeta(
      transaction_id = contract2_createWithLocalStakeholder.transaction_id.get,
      event_offset = contract2_createWithLocalStakeholder.event_offset.get,
      event_sequential_id_from = contract2_createWithLocalStakeholder.event_sequential_id,
      event_sequential_id_to = contract2_createWithLocalStakeholder.event_sequential_id,
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_immediateDivulgence,
          transactionMeta1,
          partyEntry,
          contract1_retroactiveDivulgence,
          transactionMeta2,
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
    val transactionMeta1 = DbDto.TransactionMeta(
      transaction_id = contract1_create.transaction_id.get,
      event_offset = contract1_create.event_offset.get,
      event_sequential_id_from = contract1_create.event_sequential_id,
      event_sequential_id_to = contract1_create.event_sequential_id,
    )
    val transactionMeta2 = DbDto.TransactionMeta(
      transaction_id = contract1_consumingExercise.transaction_id.get,
      event_offset = contract1_consumingExercise.event_offset.get,
      event_sequential_id_from = contract1_consumingExercise.event_sequential_id,
      event_sequential_id_to = contract1_consumingExercise.event_sequential_id,
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_create,
          transactionMeta1,
          contract1_divulgence,
          contract1_consumingExercise,
          transactionMeta2,
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
