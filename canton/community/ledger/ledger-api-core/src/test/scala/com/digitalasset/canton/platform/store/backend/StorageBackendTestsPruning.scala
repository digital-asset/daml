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

  def pruneEventsSql(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompleteReassignmentOffsets: Vector[Offset] = Vector.empty,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    executeSql { conn =>
      conn.setAutoCommit(false)
      backend.event.pruneEvents(
        pruneUpToInclusive,
        pruneAllDivulgedContracts,
        incompleteReassignmentOffsets,
      )(
        conn,
        traceContext,
      )
      conn.commit()
      conn.setAutoCommit(false)
    }

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

  it should "prune consuming, non-consuming and unassign events" in {
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
          dtoUnassign(
            offset = offset(5),
            eventSequentialId = 7L,
            contractId = hashCid("#1"),
            signatory = signatoryParty,
          ),
          DbDto.IdFilterUnassignStakeholder(7L, someTemplateId.toString, signatoryParty),
        ) ++
          Vector(
            dtoExercise(
              offset = offset(6),
              eventSequentialId = 8L,
              contractId = hashCid("#2"),
              consuming = false,
              signatory = signatoryParty,
            ),
            DbDto.IdFilterNonConsumingInformee(8L, signatoryParty),
            dtoExercise(
              offset = offset(7),
              eventSequentialId = 9L,
              contractId = hashCid("#2"),
              consuming = true,
              signatory = signatoryParty,
              actor = actorParty,
            ),
            DbDto.IdFilterConsumingStakeholder(9L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingNonStakeholderInformee(9L, actorParty),
            dtoUnassign(
              offset = offset(8),
              eventSequentialId = 10L,
              contractId = hashCid("#1"),
              signatory = signatoryParty,
            ),
            DbDto.IdFilterUnassignStakeholder(10L, someTemplateId.toString, signatoryParty),
          ),
        _,
      )
    )
    val endOffset = offset(8)

    def assertAllDataPresent(): Assertion = assertIndexDbDataSql(
      consuming = Vector(EventConsuming(6), EventConsuming(9)),
      consumingFilterStakeholder =
        Vector(FilterConsumingStakeholder(6, 3), FilterConsumingStakeholder(9, 3)),
      consumingFilterNonStakeholder =
        Vector(FilterConsumingNonStakeholder(6, 1), FilterConsumingNonStakeholder(9, 1)),
      nonConsuming = Vector(EventNonConsuming(5), EventNonConsuming(8)),
      nonConsumingFilter = Vector(FilterNonConsuming(5, 3), FilterNonConsuming(8, 3)),
      unassign = Vector(EventUnassign(7), EventUnassign(10)),
      unassignFilter = Vector(FilterUnassign(7, 3), FilterUnassign(10, 3)),
    )

    assertAllDataPresent()
    // Prune before the offset at which we ingested any events
    pruneEventsSql(offset(2), pruneAllDivulgedContracts = true)
    assertAllDataPresent()
    // Prune at offset such that there are events ingested before and after
    pruneEventsSql(offset(5), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      consuming = Vector(EventConsuming(9)),
      consumingFilterStakeholder = Vector(FilterConsumingStakeholder(9, 3)),
      consumingFilterNonStakeholder = Vector(FilterConsumingNonStakeholder(9, 1)),
      nonConsuming = Vector(EventNonConsuming(8)),
      nonConsumingFilter = Vector(FilterNonConsuming(8, 3)),
      unassign = Vector(EventUnassign(10)),
      unassignFilter = Vector(FilterUnassign(10, 3)),
    )
    // Prune at the ledger end, but setting the unassign incomplete
    pruneEventsSql(endOffset, pruneAllDivulgedContracts = true, Vector(offset(8)))
    assertIndexDbDataSql(
      unassign = Vector(EventUnassign(10)),
      unassignFilter = Vector(FilterUnassign(10, 3)),
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
      domainId = "x::sourcedomain",
    )
    // a consuming event in its own transaction
    val archive = dtoExercise(
      offset = offset(11),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      domainId = "x::sourcedomain",
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
            metaFromSingle(create),
            archive,
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, observerParty),
            metaFromSingle(archive),
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

  it should "prune a contract which was unassigned later" in {
    // a create event in its own transaction
    val create = dtoCreate(
      offset = offset(10),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
      domainId = "x::sourcedomain",
    )
    // a consuming event in its own transaction
    val unassign = dtoUnassign(
      offset = offset(11),
      eventSequentialId = 2L,
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
            metaFromSingle(create),
            unassign,
            DbDto.IdFilterUnassignStakeholder(2L, someTemplateId.toString, signatoryParty),
            metaFromSingle(unassign),
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
      unassign = Vector(EventUnassign(2)),
      unassignFilter = Vector(
        FilterUnassign(2, 2)
      ),
      txMeta = txMeta,
    )

    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011"))
    )
    // Prune at the offset of the create event
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000011"))
    )
    // Prune at the offset of the unassign event
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "not prune an active contract" in {
    val create = dtoCreate(
      offset = offset(2),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
      domainId = "x::sourcedomain",
    )
    val archiveDifferentDomain = dtoExercise(
      offset = offset(3),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      domainId = "x::targetdomain",
    )
    val archiveDifferentContractId = dtoExercise(
      offset = offset(4),
      eventSequentialId = 3L,
      consuming = true,
      contractId = hashCid("#2"),
      signatory = signatoryParty,
      domainId = "x::sourcedomain",
    )
    val unassignDifferentDomain = dtoUnassign(
      offset = offset(5),
      eventSequentialId = 4L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      sourceDomainId = "x::targetdomain",
      targetDomainId = "x::sourcedomain",
    )
    val unassignDifferentContractId = dtoUnassign(
      offset = offset(6),
      eventSequentialId = 5L,
      contractId = hashCid("#2"),
      signatory = signatoryParty,
      sourceDomainId = "x::sourcedomain",
      targetDomainId = "x::targetdomain",
    )
    val archiveAfter = dtoExercise(
      offset = offset(7),
      eventSequentialId = 6L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      domainId = "x::sourcedomain",
    )
    val unassignAfter = dtoUnassign(
      offset = offset(8),
      eventSequentialId = 7L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      sourceDomainId = "x::sourcedomain",
      targetDomainId = "x::targetdomain",
    )
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
          metaFromSingle(create),
          archiveDifferentDomain,
          DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty),
          metaFromSingle(archiveDifferentDomain),
          archiveDifferentContractId,
          DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, signatoryParty),
          metaFromSingle(archiveDifferentContractId),
          unassignDifferentDomain,
          DbDto.IdFilterUnassignStakeholder(4L, someTemplateId.toString, signatoryParty),
          metaFromSingle(unassignDifferentDomain),
          unassignDifferentContractId,
          DbDto.IdFilterUnassignStakeholder(5L, someTemplateId.toString, signatoryParty),
          metaFromSingle(unassignDifferentContractId),
          archiveAfter,
          DbDto.IdFilterConsumingStakeholder(6L, someTemplateId.toString, signatoryParty),
          metaFromSingle(archiveAfter),
          unassignAfter,
          DbDto.IdFilterUnassignStakeholder(7L, someTemplateId.toString, signatoryParty),
          metaFromSingle(unassignAfter),
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
      consuming = Vector(EventConsuming(2), EventConsuming(3), EventConsuming(6)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(2, 2),
        FilterConsumingStakeholder(3, 2),
        FilterConsumingStakeholder(6, 2),
      ),
      unassign = Vector(EventUnassign(4), EventUnassign(5), EventUnassign(7)),
      unassignFilter = Vector(
        FilterUnassign(4, 2),
        FilterUnassign(5, 2),
        FilterUnassign(7, 2),
      ),
      txMeta = txMeta,
    )
    assertAllDataPresent(
      txMeta = Vector(
        TxMeta("00000002"),
        TxMeta("00000003"),
        TxMeta("00000004"),
        TxMeta("00000005"),
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
      )
    )
    // Prune earlier
    pruneEventsSql(offset(1), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(
        TxMeta("00000002"),
        TxMeta("00000003"),
        TxMeta("00000004"),
        TxMeta("00000005"),
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
      )
    )
    // Prune at create
    pruneEventsSql(offset(2), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(
        TxMeta("00000003"),
        TxMeta("00000004"),
        TxMeta("00000005"),
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
      )
    )
    // Prune after unrelated archive and reassign events but before related ones
    pruneEventsSql(offset(6), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      create = Vector(EventCreate(1)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(1, 2),
        FilterCreateStakeholder(1, 3),
      ),
      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
      consuming = Vector(EventConsuming(6)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(6, 2)
      ),
      unassign = Vector(EventUnassign(7)),
      unassignFilter = Vector(
        FilterUnassign(7, 2)
      ),
      txMeta = Vector(
        TxMeta("00000007"),
        TxMeta("00000008"),
      ),
    )
    // Prune at the end, but following unassign is incomplete
    // (the following archive can be pruned, but the following incomplete unassign and the create cannot, to be able to look up create event for the incomplete unassigned)
    pruneEventsSql(offset(8), pruneAllDivulgedContracts = true, Vector(offset(8)))
    assertIndexDbDataSql(
      create = Vector(EventCreate(1)),
      createFilterStakeholder = Vector(
        FilterCreateStakeholder(1, 2),
        FilterCreateStakeholder(1, 3),
      ),
      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 4)),
      unassign = Vector(EventUnassign(7)),
      unassignFilter = Vector(
        FilterUnassign(7, 2)
      ),
    )
    // Prune at the end (to verify that additional events are related)
    pruneEventsSql(offset(8), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "prune an assign if archived in the same domain" in {
    // an assign event in its own transaction
    val assign = dtoAssign(
      offset = offset(10),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      sourceDomainId = "x::sourcedomain",
      targetDomainId = "x::targetdomain",
    )
    // a consuming event in its own transaction
    val archive = dtoExercise(
      offset = offset(11),
      eventSequentialId = 2L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      domainId = "x::targetdomain",
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest an assign and an archive event
    executeSql(
      ingest(
        Vector(
          dtoPartyEntry(offset(1), signatoryParty),
          dtoPartyEntry(offset(2), observerParty),
          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
        ) ++
          Vector(
            assign,
            DbDto.IdFilterAssignStakeholder(1L, someTemplateId.toString, signatoryParty),
            metaFromSingle(assign),
            archive,
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, signatoryParty),
            DbDto.IdFilterConsumingStakeholder(2L, someTemplateId.toString, observerParty),
            metaFromSingle(archive),
          ),
        _,
      )
    )

    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSql(
      assign = Vector(EventAssign(1)),
      assignFilter = Vector(FilterAssign(1, 3)),
      consuming = Vector(EventConsuming(2)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(2, 3),
        FilterConsumingStakeholder(2, 6),
      ),
      txMeta = txMeta,
    )

    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011"))
    )
    // Prune at the offset of the assign event
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000011"))
    )
    // Prune at the offset of the archive event
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "prune an assign which was unassigned in the same domain later" in {
    // an assign event in its own transaction
    val assign = dtoAssign(
      offset = offset(10),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      observer = observerParty,
      sourceDomainId = "x::sourcedomain",
      targetDomainId = "x::targetdomain",
    )

    // an unassign event in its own transaction
    val unassign = dtoUnassign(
      offset = offset(11),
      eventSequentialId = 2L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      sourceDomainId = "x::targetdomain",
      targetDomainId = "x::sourcedomain",
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest the assign and unassign event
    executeSql(
      ingest(
        Vector(
          dtoPartyEntry(offset(1), signatoryParty),
          dtoPartyEntry(offset(2), observerParty),
          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
        ) ++
          Vector(
            assign,
            DbDto.IdFilterAssignStakeholder(1L, someTemplateId.toString, signatoryParty),
            metaFromSingle(assign),
            unassign,
            DbDto.IdFilterUnassignStakeholder(2L, someTemplateId.toString, signatoryParty),
            metaFromSingle(unassign),
          ),
        _,
      )
    )

    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSql(
      assign = Vector(EventAssign(1)),
      assignFilter = Vector(FilterAssign(1, 3)),
      unassign = Vector(EventUnassign(2)),
      unassignFilter = Vector(
        FilterUnassign(2, 3)
      ),
      txMeta = txMeta,
    )

    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000010"), TxMeta("00000011"))
    )
    // Prune at the offset of the assign event
    pruneEventsSql(offset(10), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(TxMeta("00000011"))
    )
    // Prune at the offset of the unassign event
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
  }

  it should "not prune an assign" in {
    def archive(
        offsetInt: Int,
        eventSequentialId: Long,
        hashCidString: String = "#1",
        domainId: String = "x::targetdomain",
    ): Vector[DbDto] = {
      val archive = dtoExercise(
        offset = offset(offsetInt.toLong),
        eventSequentialId = eventSequentialId,
        consuming = true,
        contractId = hashCid(hashCidString),
        signatory = signatoryParty,
        domainId = domainId,
      )
      Vector(
        archive,
        DbDto
          .IdFilterConsumingStakeholder(eventSequentialId, someTemplateId.toString, signatoryParty),
        metaFromSingle(archive),
      )
    }
    def unassign(
        offsetInt: Int,
        eventSequentialId: Long,
        hashCidString: String = "#1",
        domainId: String = "x::targetdomain",
    ): Vector[DbDto] = {
      val unassign = dtoUnassign(
        offset = offset(offsetInt.toLong),
        eventSequentialId = eventSequentialId,
        contractId = hashCid(hashCidString),
        signatory = signatoryParty,
        sourceDomainId = domainId,
        targetDomainId = "x::thirddomain",
      )
      Vector(
        unassign,
        DbDto
          .IdFilterUnassignStakeholder(eventSequentialId, someTemplateId.toString, signatoryParty),
        metaFromSingle(unassign),
      )
    }

    val archiveDifferentDomainEarlierThanAssing = archive(
      offsetInt = 2,
      eventSequentialId = 1,
      domainId = "x::sourcedomain",
    )
    val unassignDifferentDomainEarlierThanAssing = unassign(
      offsetInt = 3,
      eventSequentialId = 2,
      domainId = "x::sourcedomain",
    )
    val unassignEarlierThanAssing = unassign(
      offsetInt = 4,
      eventSequentialId = 3,
    )
    val assign = dtoAssign(
      offset = offset(5),
      eventSequentialId = 4L,
      contractId = hashCid("#1"),
      signatory = signatoryParty,
      sourceDomainId = "x::sourcedomain",
      targetDomainId = "x::targetdomain",
    )
    val assignEvents = Vector(
      assign,
      DbDto.IdFilterAssignStakeholder(4L, someTemplateId.toString, signatoryParty),
      metaFromSingle(assign),
    )
    val archiveDifferentDomainEarlierThanPruning = archive(
      offsetInt = 6,
      eventSequentialId = 5,
      domainId = "x::sourcedomain",
    )
    val archiveDifferentCidEarlierThanPruning = archive(
      offsetInt = 7,
      eventSequentialId = 6,
      hashCidString = "#2",
    )
    val unassignDifferentDomainEarlierThanPruning = unassign(
      offsetInt = 8,
      eventSequentialId = 7,
      domainId = "x::sourcedomain",
    )
    val unassignDifferentCidEarlierThanPruning = unassign(
      offsetInt = 9, // pruning offset
      eventSequentialId = 8,
      hashCidString = "#2",
    )
    val archiveBeforeEnd = archive(
      offsetInt = 10,
      eventSequentialId = 9,
    )
    val unassignBeforeEnd = unassign(
      offsetInt = 11,
      eventSequentialId = 10,
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a create and archive event
    executeSql(
      ingest(
        Vector(
          Vector(dtoPartyEntry(offset(1), signatoryParty)),
          archiveDifferentDomainEarlierThanAssing,
          unassignDifferentDomainEarlierThanAssing,
          unassignEarlierThanAssing,
          assignEvents,
          archiveDifferentDomainEarlierThanPruning,
          archiveDifferentCidEarlierThanPruning,
          unassignDifferentDomainEarlierThanPruning,
          unassignDifferentCidEarlierThanPruning,
          archiveBeforeEnd,
          unassignBeforeEnd,
        ).flatten,
        _,
      )
    )

    def assertAllDataPresent(txMeta: Seq[TxMeta]): Assertion = assertIndexDbDataSql(
      assign = Vector(EventAssign(4)),
      assignFilter = Vector(FilterAssign(4, 3)),
      consuming =
        Vector(EventConsuming(1), EventConsuming(5), EventConsuming(6), EventConsuming(9)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(1, 3),
        FilterConsumingStakeholder(5, 3),
        FilterConsumingStakeholder(6, 3),
        FilterConsumingStakeholder(9, 3),
      ),
      unassign = Vector(
        EventUnassign(2),
        EventUnassign(3),
        EventUnassign(7),
        EventUnassign(8),
        EventUnassign(10),
      ),
      unassignFilter = Vector(
        FilterUnassign(2, 3),
        FilterUnassign(3, 3),
        FilterUnassign(7, 3),
        FilterUnassign(8, 3),
        FilterUnassign(10, 3),
      ),
      txMeta = txMeta,
    )
    assertAllDataPresent(
      txMeta = Vector(
        TxMeta("00000002"),
        TxMeta("00000003"),
        TxMeta("00000004"),
        TxMeta("00000005"),
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
        TxMeta("00000009"),
        TxMeta("00000010"),
        TxMeta("00000011"),
      )
    )
    // Prune earlier
    pruneEventsSql(offset(1), pruneAllDivulgedContracts = true)
    assertAllDataPresent(
      txMeta = Vector(
        TxMeta("00000002"),
        TxMeta("00000003"),
        TxMeta("00000004"),
        TxMeta("00000005"),
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
        TxMeta("00000009"),
        TxMeta("00000010"),
        TxMeta("00000011"),
      )
    )
    // Prune at assign
    pruneEventsSql(offset(5), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      assign = Vector(EventAssign(4)),
      assignFilter = Vector(FilterAssign(4, 3)),
      consuming = Vector(EventConsuming(5), EventConsuming(6), EventConsuming(9)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(5, 3),
        FilterConsumingStakeholder(6, 3),
        FilterConsumingStakeholder(9, 3),
      ),
      unassign = Vector(
        EventUnassign(7),
        EventUnassign(8),
        EventUnassign(10),
      ),
      unassignFilter = Vector(
        FilterUnassign(7, 3),
        FilterUnassign(8, 3),
        FilterUnassign(10, 3),
      ),
      txMeta = Vector(
        TxMeta("00000006"),
        TxMeta("00000007"),
        TxMeta("00000008"),
        TxMeta("00000009"),
        TxMeta("00000010"),
        TxMeta("00000011"),
      ),
    )
    // Prune after unrelated archive and reassign events but before related ones
    pruneEventsSql(offset(9), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      assign = Vector(EventAssign(4)),
      assignFilter = Vector(FilterAssign(4, 3)),
      consuming = Vector(EventConsuming(9)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(9, 3)
      ),
      unassign = Vector(
        EventUnassign(10)
      ),
      unassignFilter = Vector(
        FilterUnassign(10, 3)
      ),
      txMeta = Vector(
        TxMeta("00000010"),
        TxMeta("00000011"),
      ),
    )
    // Prune at the end, but with setting the assign incomplete
    // (the archive and the unassing cannot be pruned neither, because they belong to an incomplete activation)
    pruneEventsSql(
      offset(11),
      pruneAllDivulgedContracts = true,
      Vector(offset(5), offset(1000), offset(1001)),
    )
    assertIndexDbDataSql(
      assign = Vector(EventAssign(4)),
      assignFilter = Vector(FilterAssign(4, 3)),
      consuming = Vector(EventConsuming(9)),
      consumingFilterStakeholder = Vector(
        FilterConsumingStakeholder(9, 3)
      ),
      unassign = Vector(
        EventUnassign(10)
      ),
      unassignFilter = Vector(
        FilterUnassign(10, 3)
      ),
      txMeta = Vector.empty,
    )
    // Prune at the end, but with setting the following unassign incomplete
    // (the following archive can be pruned, but the following unassign and the assign can't, to be able to look up create event for the incomplete unassigned)
    pruneEventsSql(
      offset(11),
      pruneAllDivulgedContracts = true,
      Vector(offset(11), offset(1000), offset(1001)),
    )
    assertIndexDbDataSql(
      assign = Vector(EventAssign(4)),
      assignFilter = Vector(FilterAssign(4, 3)),
      unassign = Vector(
        EventUnassign(10)
      ),
      unassignFilter = Vector(
        FilterUnassign(10, 3)
      ),
      txMeta = Vector.empty,
    )
    // Prune at the end
    pruneEventsSql(offset(11), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql()
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
    assertIndexDbDataSql(
      create = Vector(EventCreate(1), EventCreate(2))
    )
    pruneEventsSql(offset(3), pruneAllDivulgedContracts = true)
    assertIndexDbDataSql(
      create = Vector(EventCreate(2))
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
      assign: Seq[EventAssign] = Seq.empty,
      assignFilter: Seq[FilterAssign] = Seq.empty,
      unassign: Seq[EventUnassign] = Seq.empty,
      unassignFilter: Seq[FilterUnassign] = Seq.empty,
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
    // assign
    cp(Statement.discard(queries.eventAssign shouldBe assign))
    cp(Statement.discard(queries.filterAssign shouldBe assignFilter))
    // unassign
    cp(Statement.discard(queries.eventUnassign shouldBe unassign))
    cp(Statement.discard(queries.filterUnassign shouldBe unassignFilter))
    // other
    cp(Statement.discard(queries.txMeta shouldBe txMeta))
    cp(Statement.discard(queries.completions shouldBe completion))
    cp.reportAll()
    succeed
  }
}
