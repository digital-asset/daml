// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.PruningDto._
import com.daml.platform.store.backend.PruningDto
import org.scalatest.{Assertion, Checkpoints, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Connection

private[backend] trait StorageBackendTestsPruning
    extends Matchers
    with OptionValues
    with Checkpoints
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (pruning)"

  import StorageBackendTestValues._

  private val signatoryParty = Ref.Party.assertFromString("signatory")
  private val observerParty = Ref.Party.assertFromString("observer")
  private val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
  private val actorParty = Ref.Party.assertFromString("actor")

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

  it should "prune consuming and non-consuming events" in {
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
    executeSql(
      assertIndexDbData(
        consuming = Vector(EventConsuming(6)),
        consumingFilterStakeholder = Vector(FilterConsumingStakeholder(6, 3)),
        consumingFilterNonStakeholder = Vector(FilterConsumingNonStakeholder(6, 1)),
        nonConsuming = Vector(EventNonConsuming(5)),
        nonConsumingFilter = Vector(FilterNonConsuming(5, 3)),
      )
    )
    // Prune before the offset at which we ingested our events
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    executeSql(
      assertIndexDbData(
        consuming = Vector(EventConsuming(6)),
        consumingFilterStakeholder = Vector(FilterConsumingStakeholder(6, 3)),
        consumingFilterNonStakeholder = Vector(FilterConsumingNonStakeholder(6, 1)),
        nonConsuming = Vector(EventNonConsuming(5)),
        nonConsumingFilter = Vector(FilterNonConsuming(5, 3)),
      )
    )

    // Prune at the ledger end
    executeSql(
      backend.event.pruneEvents(endOffset, pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(endOffset))
    executeSql(assertIndexDbData())
  }

  it should "prune an archived contract" in {
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

    // Make sure the entries related to the create event are visible
    executeSql(
      assertIndexDbData(
        create = Vector(EventCreate(1)),
        createFilterStakeholder = Vector(
          FilterCreateStakeholder(1, 2),
          FilterCreateStakeholder(1, 3),
        ),
        createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
        consuming = Vector(EventConsuming(2)),
        consumingFilterStakeholder = Vector(
          FilterConsumingStakeholder(2, 2),
          FilterConsumingStakeholder(2, 3),
        ),
        txMeta = Vector(TxMeta("00000001"), TxMeta("00000002")),
      )
    )

    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    // Make sure the entries related to the create event are not visible anymore
    executeSql(assertIndexDbData())
  }

  it should "not prune an active contract" in {
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

    // Make sure the entries relate to the create event are visible
    executeSql(
      assertIndexDbData(
        create = Vector(EventCreate(1)),
        createFilterStakeholder = Vector(
          FilterCreateStakeholder(1, 2),
          FilterCreateStakeholder(1, 3),
        ),
        createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
        txMeta = Vector(TxMeta("00000002")),
      )
    )

    // Prune
    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))

    // Make sure the entries related to the create event are still visible - active contracts should not be pruned
    executeSql(
      assertIndexDbData(
        create = Vector(EventCreate(1)),
        createFilterStakeholder = Vector(
          FilterCreateStakeholder(1, 2),
          FilterCreateStakeholder(1, 3),
        ),
        createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
        txMeta = Vector(TxMeta("00000002")),
      )
    )
  }

  it should "prune all retroactively and immediately divulged contracts (if pruneAllDivulgedContracts is set)" in {
    val partyName = "party"
    val divulgee = Ref.Party.assertFromString(partyName)
    val contract1_id = hashCid("#1")
    val contract2_id = hashCid("#2")

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
    executeSql(
      assertIndexDbData(
        create = Vector(EventCreate(1), EventCreate(2)),
        divulgence = Vector(EventDivulgence(3)),
      )
    )

    executeSql(
      backend.event.pruneEvents(offset(3), pruneAllDivulgedContracts = true)(
        _,
        loggingContext,
      )
    )
    executeSql(
      assertIndexDbData(
        create = Vector(EventCreate(2)),
        // Immediate divulgence for contract2 occurred after the associated party became locally hosted
        // so it is not pruned
        divulgence = Vector.empty,
      )
    )
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
    executeSql(
      assertIndexDbData(
        // Contract 1 appears as active tu `divulgee` before pruning
        create = Seq(EventCreate(1)),
        consuming = Seq(EventConsuming(3)),
        divulgence = Seq(
          EventDivulgence(2),
          // Contract 2 appears as active tu `divulgee` before pruning
          EventDivulgence(4),
        ),
      )
    )
    executeSql(
      backend.event.pruneEvents(offset(4), pruneAllDivulgedContracts = false)(
        _,
        loggingContext,
      )
    )
    executeSql(
      assertIndexDbData(
        // Contract 1 should not be visible anymore to `divulgee` after pruning
        create = Seq.empty,
        consuming = Seq.empty,
        divulgence = Seq(
          // Contract 2 did not have a locally stored exercise event
          // hence its divulgence is not pruned - appears active to `divulgee`
          EventDivulgence(4)
        ),
      )
    )
  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitter = someParty,
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    // Ingest a completion
    executeSql(ingest(Vector(completion), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    // Make sure the completion is visible
    executeSql(
      assertIndexDbData(
        completion = Seq(PruningDto.Completion("00000001"))
      )
    )

    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, loggingContext))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(1)))

    // Make sure the completion is not visible anymore
    executeSql(
      assertIndexDbData(
        completion = Seq.empty
      )
    )
  }

  /** Asserts the content of the tables subject to pruning.
    * Be default asserts the tables are empty.
    */
  def assertIndexDbData(
      create: Seq[EventCreate] = Seq.empty,
      createFilterStakeholder: Seq[FilterCreateStakeholder] = Seq.empty,
      createFilterNonStakeholder: Seq[FilterCreateNonStakeholder] = Seq.empty,
      consuming: Seq[EventConsuming] = Seq.empty,
      consumingFilterStakeholder: Seq[FilterConsumingStakeholder] = Seq.empty,
      consumingFilterNonStakeholder: Seq[FilterConsumingNonStakeholder] = Seq.empty,
      nonConsuming: Seq[EventNonConsuming] = Seq.empty,
      nonConsumingFilter: Seq[FilterNonConsuming] = Seq.empty,
      divulgence: Seq[EventDivulgence] = Seq.empty,
      txMeta: Seq[TxMeta] = Seq.empty,
      completion: Seq[Completion] = Seq.empty,
  )(c: Connection): Assertion = {
    implicit val _c: Connection = c
    val queries = backend.pruningDtoQueries
    val cp = new Checkpoint
    // create
    queries.eventCreate shouldBe create
    queries.filterCreateStakeholder shouldBe createFilterStakeholder
    queries.filterCreateNonStakeholder shouldBe createFilterNonStakeholder
    // consuming
    queries.eventConsuming shouldBe consuming
    queries.filterConsumingStakeholder shouldBe consumingFilterStakeholder
    queries.filterConsumingNonStakeholder shouldBe consumingFilterNonStakeholder
    // non-consuming
    queries.eventNonConsuming shouldBe nonConsuming
    queries.filterNonConsuming shouldBe nonConsumingFilter
    // other
    queries.eventDivulgence shouldBe divulgence
    queries.txMeta shouldBe txMeta
    queries.completions shouldBe completion
    cp.reportAll()
    succeed
  }
}
