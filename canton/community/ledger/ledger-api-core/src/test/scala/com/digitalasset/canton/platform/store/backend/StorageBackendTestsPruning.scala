// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.scalautil.Statement
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.store.backend.PruningDto.*
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Checkpoints, OptionValues}

private[backend] trait StorageBackendTestsPruning
    extends Matchers
    with OptionValues
    with Checkpoints
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (pruning)"

  import StorageBackendTestValues.*

  private val signatoryParty = Ref.Party.assertFromString("signatory")
  private val observerParty = Ref.Party.assertFromString("observer")
  private val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
  private val actorParty = Ref.Party.assertFromString("actor")

  def pruneEventsSql(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      traceContext: TraceContext
  ): Unit =
    executeSql(
      backend.event.pruneEvents(pruneUpToInclusive, pruneAllDivulgedContracts)(_, traceContext)
    )

  it should "correctly update the pruning offset" in {
    val offset_1 = offset(3)
    val offset_2 = offset(2)
    val offset_3 = offset(4)

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
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
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
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
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          dtoExercise(
            offset = offset(3),
            eventSequentialId = 5L,
            contractId = hashCid("#1"),
            consuming = false,
            signatory = signatoryParty,
          ),
          DbDto.IdFilterNonConsumingInformee(5L, signatoryParty),
          dtoExercise(
            offset = offset(4),
            eventSequentialId = 6L,
            contractId = hashCid("#1"),
            consuming = true,
            signatory = signatoryParty,
            actor = actorParty,
          ),
          DbDto.IdFilterConsumingStakeholder(6L, someTemplateId.toString, signatoryParty),
          DbDto.IdFilterConsumingNonStakeholderInformee(6L, actorParty),
        ) ++
          Vector(
            dtoExercise(
              offset = offset(5),
              eventSequentialId = 7L,
              contractId = hashCid("#2"),
              consuming = false,
              signatory = signatoryParty,
            ),
            DbDto.IdFilterNonConsumingInformee(7L, signatoryParty),
            dtoExercise(
              offset = offset(6),
              eventSequentialId = 8L,
              contractId = hashCid("#2"),
              consuming = true,
              signatory = signatoryParty,
              actor = actorParty,
            ),
            DbDto.IdFilterConsumingStakeholder(8L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingNonStakeholderInformee(8L, actorParty),
          ),
        _,
      )
    )
    val endOffset = offset(6)
    val endId = 8L

    def assertAllDataPresent(): Assertion = assertIndexDbDataSql(
      consuming = Vector(EventConsuming(6), EventConsuming(8)),
      consumingFilterStakeholder =
        Vector(FilterConsumingStakeholder(6, 3), FilterConsumingStakeholder(8, 3)),
      consumingFilterNonStakeholder =
        Vector(FilterConsumingNonStakeholder(6, 1), FilterConsumingNonStakeholder(8, 1)),
      nonConsuming = Vector(EventNonConsuming(5), EventNonConsuming(7)),
      nonConsumingFilter = Vector(FilterNonConsuming(5, 3), FilterNonConsuming(7, 3)),
    )

    executeSql(updateLedgerEnd(endOffset, endId))
    assertAllDataPresent()
    // Prune before the offset at which we ingested any events
    pruneEventsSql(offset(2), pruneAllDivulgedContracts = true)
    assertAllDataPresent()
    // Prune at offset such that there are events ingested before and after
    pruneEventsSql(offset(4), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      consuming = Vector(EventConsuming(8)),
      consumingFilterStakeholder = Vector(FilterConsumingStakeholder(8, 3)),
      consumingFilterNonStakeholder = Vector(FilterConsumingNonStakeholder(8, 1)),
      nonConsuming = Vector(EventNonConsuming(7)),
      nonConsumingFilter = Vector(FilterNonConsuming(7, 3)),
    )
    // Prune at the ledger end
    pruneEventsSql(endOffset, pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "prune an archived contract" in {
    // a create event in its own transaction
    val create = dtoCreate(
      offset = offset(10),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    // a consuming event in its own transaction
    val archive = dtoExercise(
      offset = offset(11),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          // Allocating parties so that the contracts we create later are not considered to be a case of immediate divulgence
          dtoPartyEntry(offset(1), signatoryParty),
          dtoPartyEntry(offset(2), observerParty),
          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
        ) ++
          Vector(
            create,
            DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, observerParty),
            DbDto.IdFilterCreateNonStakeholderInformee(1L, nonStakeholderInformeeParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(create),
              event_offset = create.event_offset.value,
              event_sequential_id_first = create.event_sequential_id,
              event_sequential_id_last = create.event_sequential_id,
            ),
            archive,
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, observerParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(archive),
              event_offset = archive.event_offset.value,
              event_sequential_id_first = archive.event_sequential_id,
              event_sequential_id_last = archive.event_sequential_id,
            ),
          ),
        _,
      )
    )

    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSql(
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
      txMeta = txMeta,
    )

    executeSql(updateLedgerEnd(offset(2), 2L))
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011"))
    )
    // Prune at the offset of the create event
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000011"))
    )
    // Prune at the offset of the archive event
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      txMeta = Vector.empty
    )
  }

  it should "not prune an active contract" in {
    val create = dtoCreate(
      offset = offset(2),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    val createTxId = dtoTransactionId(create)
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          dtoPartyEntry(offset(1), signatoryParty),
          create,
          DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatoryParty),
          DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, observerParty),
          DbDto.IdFilterCreateNonStakeholderInformee(1L, nonStakeholderInformeeParty),
          DbDto.TransactionMeta(
            transaction_id = createTxId,
            event_offset = create.event_offset.value,
            event_sequential_id_first = create.event_sequential_id,
            event_sequential_id_last = create.event_sequential_id,
          ),
        ),
        _,
      )
    )

    def assertAllDataPresent(txMeta: Seq[TxMeta]): Assertion = assertIndexDbDataSql(
      create = Vector(EventCreate(1)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(1, 2),
        FilterCreateStakeholder(1, 3),
      ),
      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
      txMeta = txMeta,
    )
    executeSql(updateLedgerEnd(offset(2), 1L))
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000002"))
    )
    // Prune
    pruneEventsSql(offset(2), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector.empty
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
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_immediateDivulgence,
          partyEntry,
          contract2_createWithLocalStakeholder,
        ),
        _,
      )
    )
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    assertIndexDbDataSql(
      create = Vector(EventCreate(1), EventCreate(2))
    )
    pruneEventsSql(offset(3), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      create = Vector(EventCreate(2))
    )
  }

  it should "only prune retroactively divulged contracts if there exists an associated consuming exercise (if pruneAllDivulgedContracts is not set)" in {
    val signatory = "signatory"
    val contract1_id = hashCid("#1")
    val contract1_create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contract1_id,
      signatory = signatory,
    )
    val contract1_consumingExercise = dtoExercise(
      offset = offset(3),
      eventSequentialId = 3L,
      consuming = true,
      contractId = contract1_id,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest
    executeSql(
      ingest(
        Vector(
          contract1_create,
          contract1_consumingExercise,
        ),
        _,
      )
    )
    // Set the ledger end past the last ingested event so we can prune up to it inclusively
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )
    assertIndexDbDataSql(
      // Contract 1 appears as active tu `divulgee` before pruning
      create = Seq(EventCreate(1)),
      consuming = Seq(EventConsuming(3)),
    )
    pruneEventsSql(offset(4), pruneAllDivulgedContracts = false)
    assertIndexDbDataSql(
      // Contract 1 should not be visible anymore to `divulgee` after pruning
      create = Seq.empty,
      consuming = Seq.empty,
    )
  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitter = someParty,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a completion
    executeSql(ingest(Vector(completion), _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    assertIndexDbDataSql(completion = Seq(PruningDto.Completion("00000001")))
    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, TraceContext.empty))
    assertIndexDbDataSql(completion = Seq.empty)
  }

  /** Asserts the content of the tables subject to pruning.
    * Be default asserts the tables are empty.
    */
  def assertIndexDbDataSql(
      create: Seq[EventCreate] = Seq.empty,
      createFilterStakeholder: Seq[FilterCreateStakeholder] = Seq.empty,
      createFilterNonStakeholder: Seq[FilterCreateNonStakeholder] = Seq.empty,
      consuming: Seq[EventConsuming] = Seq.empty,
      consumingFilterStakeholder: Seq[FilterConsumingStakeholder] = Seq.empty,
      consumingFilterNonStakeholder: Seq[FilterConsumingNonStakeholder] = Seq.empty,
      nonConsuming: Seq[EventNonConsuming] = Seq.empty,
      nonConsumingFilter: Seq[FilterNonConsuming] = Seq.empty,
      txMeta: Seq[TxMeta] = Seq.empty,
      completion: Seq[Completion] = Seq.empty,
  ): Assertion = executeSql { implicit c =>
    val queries = backend.pruningDtoQueries
    val cp = new Checkpoint
    // create
    cp(Statement.discard(queries.eventCreate shouldBe create))
    cp(Statement.discard(queries.filterCreateStakeholder shouldBe createFilterStakeholder))
    cp(Statement.discard(queries.filterCreateNonStakeholder shouldBe createFilterNonStakeholder))
    // consuming
    cp(Statement.discard(queries.eventConsuming shouldBe consuming))
    cp(Statement.discard(queries.filterConsumingStakeholder shouldBe consumingFilterStakeholder))
    cp(
      Statement.discard(
        queries.filterConsumingNonStakeholder shouldBe consumingFilterNonStakeholder
      )
    )
    // non-consuming
    cp(Statement.discard(queries.eventNonConsuming shouldBe nonConsuming))
    cp(Statement.discard(queries.filterNonConsuming shouldBe nonConsumingFilter))
    // other
    cp(Statement.discard(queries.txMeta shouldBe txMeta))
    cp(Statement.discard(queries.completions shouldBe completion))
    cp.reportAll()
    succeed
  }
}
