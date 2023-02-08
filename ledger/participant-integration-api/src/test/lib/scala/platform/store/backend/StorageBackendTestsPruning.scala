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
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.scalautil.Statement

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

  def pruneEventsSql(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Unit =
    executeSql(
      backend.event.pruneEvents(pruneUpToInclusive, pruneAllDivulgedContracts)(_, loggingContext)
    )

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
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
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
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
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
              event_offset = create.event_offset.get,
              event_sequential_id_first = create.event_sequential_id,
              event_sequential_id_last = create.event_sequential_id,
            ),
            archive,
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, observerParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(archive),
              event_offset = archive.event_offset.get,
              event_sequential_id_first = archive.event_sequential_id,
              event_sequential_id_last = archive.event_sequential_id,
            ),
          ),
        _,
      )
    )

    def assertAllDataPresent(): Assertion = assertIndexDbDataSql(
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
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011")),
    )

    executeSql(updateLedgerEnd(offset(2), 2L))
    assertAllDataPresent()
    // Prune at the offset of the create event
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent()
    // Prune at the offset of the archive event
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "prune participant_transaction_meta table when all its contracts are archived" in {
    // A transaction with two contracts
    val create1 = dtoCreate(
      offset = offset(10),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    val create2 = dtoCreate(
      offset = offset(10),
      eventSequentialId = 2L,
      contractId = hashCid("#2"),
      signatory = signatoryParty,
      observer = observerParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
    )
    // A transaction with an archive of the first contract
    val archive1 = dtoExercise(
      offset = offset(11),
      eventSequentialId = 3L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
    )
    // A transaction with an archive of the second contract
    val archive2 = dtoExercise(
      offset = offset(12),
      eventSequentialId = 4L,
      consuming = true,
      contractId = hashCid("#2"),
      signatory = signatoryParty,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
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
            // Transaction 1
            create1,
            DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, observerParty),
            DbDto.IdFilterCreateNonStakeholderInformee(1L, nonStakeholderInformeeParty),
            create2,
            DbDto.IdFilterCreateStakeholder(2L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterCreateStakeholder(2L, someTemplateId.toString, observerParty),
            DbDto.IdFilterCreateNonStakeholderInformee(2L, nonStakeholderInformeeParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(create1),
              event_offset = create1.event_offset.get,
              event_sequential_id_first = 1L,
              event_sequential_id_last = 2L,
            ),
          ) ++
          Vector(
            // Transaction 2
            archive1,
            DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, observerParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(archive1),
              event_offset = archive1.event_offset.get,
              event_sequential_id_first = 3L,
              event_sequential_id_last = 3L,
            ),
          ) ++
          Vector(
            // Transaction 3
            archive2,
            DbDto.IdFilterConsumingStakeholder(4L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(4L, someTemplateId.toString, observerParty),
            DbDto.TransactionMeta(
              transaction_id = dtoTransactionId(archive2),
              event_offset = archive2.event_offset.get,
              event_sequential_id_first = 4L,
              event_sequential_id_last = 4L,
            ),
          ),
        _,
      )
    )

    def assertAllDataPresent(): Assertion = assertIndexDbDataSql(
      create = Vector(EventCreate(1), EventCreate(2)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(1, 2),
        FilterCreateStakeholder(1, 3),
        FilterCreateStakeholder(2, 2),
        FilterCreateStakeholder(2, 3),
      ),
      createFilterNonStakeholder =
        Vector(FilterCreateNonStakeholder(1, 4), FilterCreateNonStakeholder(2, 4)),
      consuming = Vector(EventConsuming(3), EventConsuming(4)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(3, 2),
        FilterConsumingStakeholder(3, 3),
        FilterConsumingStakeholder(4, 2),
        FilterConsumingStakeholder(4, 3),
      ),
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011"), TxMeta("00000012")),
    )

    executeSql(updateLedgerEnd(offset(3), 4L))
    assertAllDataPresent()
    // Prune at offset where there are no archives
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent()
    // Prune at offset with the first archive
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      create = Vector(EventCreate(2)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(2, 2),
        FilterCreateStakeholder(2, 3),
      ),
      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(2, 4)),
      consuming = Vector(EventConsuming(4)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(4, 2),
        FilterConsumingStakeholder(4, 3),
      ),
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000012")),
    )
    // Prune at offset with the second archive
    pruneEventsSql(offset(12), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
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
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
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
            event_offset = create.event_offset.get,
            event_sequential_id_first = create.event_sequential_id,
            event_sequential_id_last = create.event_sequential_id,
          ),
        ),
        _,
      )
    )

    def assertAllDataPresent(): Assertion = assertIndexDbDataSql(
      create = Vector(EventCreate(1)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(1, 2),
        FilterCreateStakeholder(1, 3),
      ),
      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
      txMeta = Vector(TxMeta("00000002")),
    )
    executeSql(updateLedgerEnd(offset(2), 1L))
    assertAllDataPresent()
    // Prune
    pruneEventsSql(offset(2), pruneAllDivulgedContracts = true)
    assertAllDataPresent()
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
    assertIndexDbDataSql(
      create = Vector(EventCreate(1), EventCreate(2)),
      divulgence = Vector(EventDivulgence(3)),
    )
    pruneEventsSql(offset(3), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      create = Vector(EventCreate(2)),
      // Immediate divulgence for contract2 occurred after the associated party became locally hosted
      // so it is not pruned
      divulgence = Vector.empty,
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
    assertIndexDbDataSql(
      // Contract 1 appears as active tu `divulgee` before pruning
      create = Seq(EventCreate(1)),
      consuming = Seq(EventConsuming(3)),
      divulgence = Seq(
        EventDivulgence(2),
        // Contract 2 appears as active tu `divulgee` before pruning
        EventDivulgence(4),
      ),
    )
    pruneEventsSql(offset(4), pruneAllDivulgedContracts = false)
    assertIndexDbDataSql(
      // Contract 1 should not be visible anymore to `divulgee` after pruning
      create = Seq.empty,
      consuming = Seq.empty,
      divulgence = Seq(
        // Contract 2 did not have a locally stored exercise event
        // hence its divulgence is not pruned - appears active to `divulgee`
        EventDivulgence(4)
      ),
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
    assertIndexDbDataSql(completion = Seq(PruningDto.Completion("00000001")))
    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, loggingContext))
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
      divulgence: Seq[EventDivulgence] = Seq.empty,
      txMeta: Seq[TxMeta] = Seq.empty,
      completion: Seq[Completion] = Seq.empty,
  ): Assertion = executeSql { c: Connection =>
    implicit val _c: Connection = c
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
    cp(Statement.discard(queries.eventDivulgence shouldBe divulgence))
    cp(Statement.discard(queries.txMeta shouldBe txMeta))
    cp(Statement.discard(queries.completions shouldBe completion))
    cp.reportAll()
    succeed
  }
}
