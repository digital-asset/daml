// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.Statement
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.PruningDto.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
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

//  private val signatoryParty = Ref.Party.assertFromString("signatory")
//  private val observerParty = Ref.Party.assertFromString("observer")
//  private val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
//  private val actorParty = Ref.Party.assertFromString("actor")

  def pruneEventsSql(
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      traceContext: TraceContext
  ): Unit =
    executeSql { conn =>
      conn.setAutoCommit(false)
      backend.event.pruneEvents(
        previousPruneUpToInclusive = previousPruneUpToInclusive,
        previousIncompleteReassignmentOffsets = previousIncompleteReassignmentOffsets,
        pruneUpToInclusive = pruneUpToInclusive,
        incompleteReassignmentOffsets = incompleteReassignmentOffsets,
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

// TODO(i28539) analyse if additional unit tests needed, and implement them with the new schema
//  it should "prune consuming, non-consuming and unassign events" in {
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest a create and archive event
//    executeSql(
//      ingest(
//        Vector(
//          dtoExerciseLegacy(
//            offset = offset(3),
//            eventSequentialId = 5L,
//            contractId = hashCid("#1"),
//            consuming = false,
//            signatory = signatoryParty,
//          ),
//          DbDto.IdFilterNonConsumingInformee(
//            5L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          dtoExerciseLegacy(
//            offset = offset(4),
//            eventSequentialId = 6L,
//            contractId = hashCid("#1"),
//            consuming = true,
//            signatory = signatoryParty,
//            actor = actorParty,
//          ),
//          DbDto.IdFilterConsumingStakeholder(
//            6L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          DbDto.IdFilterConsumingNonStakeholderInformee(
//            6L,
//            someTemplateId.toString,
//            actorParty,
//            first_per_sequential_id = true,
//          ),
//          dtoUnassignLegacy(
//            offset = offset(5),
//            eventSequentialId = 7L,
//            contractId = hashCid("#1"),
//            signatory = signatoryParty,
//          ),
//          DbDto.IdFilterUnassignStakeholder(
//            7L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//        ) ++
//          Vector(
//            dtoExerciseLegacy(
//              offset = offset(6),
//              eventSequentialId = 8L,
//              contractId = hashCid("#2"),
//              consuming = false,
//              signatory = signatoryParty,
//            ),
//            DbDto.IdFilterNonConsumingInformee(
//              8L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            dtoExerciseLegacy(
//              offset = offset(7),
//              eventSequentialId = 9L,
//              contractId = hashCid("#2"),
//              consuming = true,
//              signatory = signatoryParty,
//              actor = actorParty,
//            ),
//            DbDto.IdFilterConsumingStakeholder(
//              9L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            DbDto.IdFilterConsumingNonStakeholderInformee(
//              9L,
//              someTemplateId.toString,
//              actorParty,
//              first_per_sequential_id = true,
//            ),
//            dtoUnassignLegacy(
//              offset = offset(8),
//              eventSequentialId = 10L,
//              contractId = hashCid("#1"),
//              signatory = signatoryParty,
//            ),
//            DbDto.IdFilterUnassignStakeholder(
//              10L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//          ),
//        _,
//      )
//    )
//    val endOffset = offset(8)
//
//    def assertAllDataPresent(): Assertion = assertIndexDbDataSqlLegacy(
//      consuming = Vector(EventConsuming(6), EventConsuming(9)),
//      consumingFilterStakeholder =
//        Vector(FilterConsumingStakeholder(6, 6), FilterConsumingStakeholder(9, 6)),
//      consumingFilterNonStakeholder =
//        Vector(FilterConsumingNonStakeholder(6, 1), FilterConsumingNonStakeholder(9, 1)),
//      nonConsuming = Vector(EventNonConsuming(5), EventNonConsuming(8)),
//      nonConsumingFilter = Vector(FilterNonConsuming(5, 6), FilterNonConsuming(8, 6)),
//      unassign = Vector(EventUnassign(7), EventUnassign(10)),
//      unassignFilter = Vector(FilterUnassign(7, 6), FilterUnassign(10, 6)),
//    )
//
//    assertAllDataPresent()
//    // Prune before the offset at which we ingested any events
//    pruneEventsSqlLegacy(offset(2))
//    assertAllDataPresent()
//    // Prune at offset such that there are events ingested before and after
//    pruneEventsSqlLegacy(offset(5))
//    assertIndexDbDataSqlLegacy(
//      consuming = Vector(EventConsuming(9)),
//      consumingFilterStakeholder = Vector(FilterConsumingStakeholder(9, 6)),
//      consumingFilterNonStakeholder = Vector(FilterConsumingNonStakeholder(9, 1)),
//      nonConsuming = Vector(EventNonConsuming(8)),
//      nonConsumingFilter = Vector(FilterNonConsuming(8, 6)),
//      unassign = Vector(EventUnassign(10)),
//      unassignFilter = Vector(FilterUnassign(10, 6)),
//    )
//    // Prune at the ledger end, but setting the unassign incomplete
//    pruneEventsSqlLegacy(endOffset, Vector(offset(8)))
//    assertIndexDbDataSqlLegacy(
//      unassign = Vector(EventUnassign(10)),
//      unassignFilter = Vector(FilterUnassign(10, 6)),
//    )
//    // Prune at the ledger end
//    pruneEventsSqlLegacy(endOffset)
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "prune an archived contract" in {
//    // a create event in its own transaction
//    val create = dtoCreateLegacy(
//      offset = offset(10),
//      eventSequentialId = 1L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      observer = observerParty,
//      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
//      synchronizerId = someSynchronizerId,
//    )
//    // a consuming event in its own transaction
//    val archive = dtoExerciseLegacy(
//      offset = offset(11),
//      eventSequentialId = 2L,
//      consuming = true,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      synchronizerId = someSynchronizerId,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest a create and archive event
//    executeSql(
//      ingest(
//        Vector(
//          // Allocating parties so that the contracts we create later are not considered to be a case of immediate divulgence
//          dtoPartyEntry(offset(1), signatoryParty),
//          dtoPartyEntry(offset(2), observerParty),
//          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
//        ) ++
//          Vector(
//            create,
//            DbDto.IdFilterCreateStakeholder(
//              1L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            DbDto.IdFilterCreateStakeholder(
//              1L,
//              someTemplateId.toString,
//              observerParty,
//              first_per_sequential_id = false,
//            ),
//            DbDto.IdFilterCreateNonStakeholderInformee(
//              1L,
//              someTemplateId.toString,
//              nonStakeholderInformeeParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(create),
//            archive,
//            DbDto.IdFilterConsumingStakeholder(
//              2L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            DbDto.IdFilterConsumingStakeholder(
//              2L,
//              someTemplateId.toString,
//              observerParty,
//              first_per_sequential_id = false,
//            ),
//            metaFromSingle(archive),
//          ),
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1)),
//      createFilterStakeholder = Vector(
//        FilterCreateStakeholder(1, 3),
//        FilterCreateStakeholder(1, 4),
//      ),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 5)),
//      consuming = Vector(EventConsuming(2)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(2, 3),
//        FilterConsumingStakeholder(2, 4),
//      ),
//      txMeta = txMeta,
//    )
//
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(10), TxMeta(11))
//    )
//    // Prune at the offset of the create event
//    pruneEventsSqlLegacy(offset(10))
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(11))
//    )
//    // Prune at the offset of the archive event
//    pruneEventsSqlLegacy(offset(11))
//    assertIndexDbDataSqlLegacy(
//      txMeta = Vector.empty
//    )
//  }
//
//  it should "prune a contract which was unassigned later" in {
//    // a create event in its own transaction
//    val create = dtoCreateLegacy(
//      offset = offset(10),
//      eventSequentialId = 1L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      observer = observerParty,
//      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
//      synchronizerId = someSynchronizerId,
//    )
//    // a consuming event in its own transaction
//    val unassign = dtoUnassignLegacy(
//      offset = offset(11),
//      eventSequentialId = 2L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest a create and archive event
//    executeSql(
//      ingest(
//        Vector(
//          // Allocating parties so that the contracts we create later are not considered to be a case of immediate divulgence
//          dtoPartyEntry(offset(1), signatoryParty),
//          dtoPartyEntry(offset(2), observerParty),
//          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
//        ) ++
//          Vector(
//            create,
//            DbDto.IdFilterCreateStakeholder(
//              1L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            DbDto.IdFilterCreateStakeholder(
//              1L,
//              someTemplateId.toString,
//              observerParty,
//              first_per_sequential_id = false,
//            ),
//            DbDto.IdFilterCreateNonStakeholderInformee(
//              1L,
//              someTemplateId.toString,
//              nonStakeholderInformeeParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(create),
//            unassign,
//            DbDto.IdFilterUnassignStakeholder(
//              2L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(unassign),
//          ),
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1)),
//      createFilterStakeholder = Vector(
//        FilterCreateStakeholder(1, 3),
//        FilterCreateStakeholder(1, 4),
//      ),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 5)),
//      unassign = Vector(EventUnassign(2)),
//      unassignFilter = Vector(FilterUnassign(2, 3)),
//      txMeta = txMeta,
//    )
//
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(10), TxMeta(11))
//    )
//    // Prune at the offset of the create event
//    pruneEventsSqlLegacy(offset(10))
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(11))
//    )
//    // Prune at the offset of the unassign event
//    pruneEventsSqlLegacy(offset(11))
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "not prune an active contract" in {
//    val create = dtoCreateLegacy(
//      offset = offset(2),
//      eventSequentialId = 1L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      nonStakeholderInformees = Set(nonStakeholderInformeeParty),
//      synchronizerId = someSynchronizerId,
//    )
//    val archiveDifferentSynchronizer = dtoExerciseLegacy(
//      offset = offset(3),
//      eventSequentialId = 2L,
//      consuming = true,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      synchronizerId = someSynchronizerId2,
//    )
//    val archiveDifferentContractId = dtoExerciseLegacy(
//      offset = offset(4),
//      eventSequentialId = 3L,
//      consuming = true,
//      contractId = hashCid("#2"),
//      signatory = signatoryParty,
//      synchronizerId = someSynchronizerId,
//    )
//    val unassignDifferentSynchronizer = dtoUnassignLegacy(
//      offset = offset(5),
//      eventSequentialId = 4L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      sourceSynchronizerId = someSynchronizerId2,
//      targetSynchronizerId = someSynchronizerId,
//    )
//    val unassignDifferentContractId = dtoUnassignLegacy(
//      offset = offset(6),
//      eventSequentialId = 5L,
//      contractId = hashCid("#2"),
//      signatory = signatoryParty,
//      sourceSynchronizerId = someSynchronizerId,
//      targetSynchronizerId = someSynchronizerId2,
//    )
//    val archiveAfter = dtoExerciseLegacy(
//      offset = offset(7),
//      eventSequentialId = 6L,
//      consuming = true,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      synchronizerId = someSynchronizerId,
//    )
//    val unassignAfter = dtoUnassignLegacy(
//      offset = offset(8),
//      eventSequentialId = 7L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      sourceSynchronizerId = someSynchronizerId,
//      targetSynchronizerId = someSynchronizerId2,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest a create and archive event
//    executeSql(
//      ingest(
//        Vector(
//          dtoPartyEntry(offset(1), signatoryParty),
//          create,
//          DbDto.IdFilterCreateStakeholder(
//            1L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          DbDto.IdFilterCreateStakeholder(
//            1L,
//            someTemplateId.toString,
//            observerParty,
//            first_per_sequential_id = false,
//          ),
//          DbDto.IdFilterCreateNonStakeholderInformee(
//            1L,
//            someTemplateId.toString,
//            nonStakeholderInformeeParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(create),
//          archiveDifferentSynchronizer,
//          DbDto.IdFilterConsumingStakeholder(
//            2L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(archiveDifferentSynchronizer),
//          archiveDifferentContractId,
//          DbDto.IdFilterConsumingStakeholder(
//            3L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(archiveDifferentContractId),
//          unassignDifferentSynchronizer,
//          DbDto.IdFilterUnassignStakeholder(
//            4L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(unassignDifferentSynchronizer),
//          unassignDifferentContractId,
//          DbDto.IdFilterUnassignStakeholder(
//            5L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(unassignDifferentContractId),
//          archiveAfter,
//          DbDto.IdFilterConsumingStakeholder(
//            6L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(archiveAfter),
//          unassignAfter,
//          DbDto.IdFilterUnassignStakeholder(
//            7L,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//          metaFromSingle(unassignAfter),
//        ),
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Seq[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1)),
//      createFilterStakeholder = Vector(
//        FilterCreateStakeholder(1, 3),
//        FilterCreateStakeholder(1, 4),
//      ),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 5)),
//      consuming = Vector(EventConsuming(2), EventConsuming(3), EventConsuming(6)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(2, 3),
//        FilterConsumingStakeholder(3, 3),
//        FilterConsumingStakeholder(6, 3),
//      ),
//      unassign = Vector(EventUnassign(4), EventUnassign(5), EventUnassign(7)),
//      unassignFilter = Vector(
//        FilterUnassign(4, 3),
//        FilterUnassign(5, 3),
//        FilterUnassign(7, 3),
//      ),
//      txMeta = txMeta,
//    )
//    assertAllDataPresent(
//      txMeta = Vector(
//        TxMeta(2),
//        TxMeta(3),
//        TxMeta(4),
//        TxMeta(5),
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//      )
//    )
//    // Prune earlier
//    pruneEventsSqlLegacy(offset(1))
//    assertAllDataPresent(
//      txMeta = Vector(
//        TxMeta(2),
//        TxMeta(3),
//        TxMeta(4),
//        TxMeta(5),
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//      )
//    )
//    // Prune at create
//    pruneEventsSqlLegacy(offset(2))
//    assertAllDataPresent(
//      txMeta = Vector(
//        TxMeta(3),
//        TxMeta(4),
//        TxMeta(5),
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//      )
//    )
//    // Prune after unrelated archive and reassign events but before related ones
//    pruneEventsSqlLegacy(offset(6))
//    assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1)),
//      createFilterStakeholder = Vector(
//        FilterCreateStakeholder(1, 3),
//        FilterCreateStakeholder(1, 4),
//      ),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 5)),
//      consuming = Vector(EventConsuming(6)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(6, 3)
//      ),
//      unassign = Vector(EventUnassign(7)),
//      unassignFilter = Vector(FilterUnassign(7, 3)),
//      txMeta = Vector(
//        TxMeta(7),
//        TxMeta(8),
//      ),
//    )
//    // Prune at the end, but following unassign is incomplete
//    // (the following archive can be pruned, but the following incomplete unassign and the create cannot, to be able to look up create event for the incomplete unassigned)
//    pruneEventsSqlLegacy(offset(8), Vector(offset(8)))
//    assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1)),
//      createFilterStakeholder = Vector(
//        FilterCreateStakeholder(1, 3),
//        FilterCreateStakeholder(1, 4),
//      ),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 5)),
//      unassign = Vector(EventUnassign(7)),
//      unassignFilter = Vector(FilterUnassign(7, 3)),
//    )
//    // Prune at the end (to verify that additional events are related)
//    pruneEventsSqlLegacy(offset(8))
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "prune an assign if archived in the same synchronizer" in {
//    // an assign event in its own transaction
//    val assign = dtoAssignLegacy(
//      offset = offset(10),
//      eventSequentialId = 1L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      observer = observerParty,
//      sourceSynchronizerId = someSynchronizerId,
//      targetSynchronizerId = someSynchronizerId2,
//    )
//    // a consuming event in its own transaction
//    val archive = dtoExerciseLegacy(
//      offset = offset(11),
//      eventSequentialId = 2L,
//      consuming = true,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      synchronizerId = someSynchronizerId2,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest an assign and an archive event
//    executeSql(
//      ingest(
//        Vector(
//          dtoPartyEntry(offset(1), signatoryParty),
//          dtoPartyEntry(offset(2), observerParty),
//          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
//        ) ++
//          Vector(
//            assign,
//            DbDto.IdFilterAssignStakeholder(
//              1L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(assign),
//            archive,
//            DbDto.IdFilterConsumingStakeholder(
//              2L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            DbDto.IdFilterConsumingStakeholder(
//              2L,
//              someTemplateId.toString,
//              observerParty,
//              first_per_sequential_id = false,
//            ),
//            metaFromSingle(archive),
//          ),
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(1)),
//      assignFilter = Vector(FilterAssign(1, 6)),
//      consuming = Vector(EventConsuming(2)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(2, 6),
//        FilterConsumingStakeholder(2, 9),
//      ),
//      txMeta = txMeta,
//    )
//
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(10), TxMeta(11))
//    )
//    // Prune at the offset of the assign event
//    pruneEventsSqlLegacy(offset(10))
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(11))
//    )
//    // Prune at the offset of the archive event
//    pruneEventsSqlLegacy(offset(11))
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "prune an assign which was unassigned in the same synchronizer later" in {
//    // an assign event in its own transaction
//    val assign = dtoAssignLegacy(
//      offset = offset(10),
//      eventSequentialId = 1L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      observer = observerParty,
//      sourceSynchronizerId = someSynchronizerId,
//      targetSynchronizerId = someSynchronizerId2,
//    )
//
//    // an unassign event in its own transaction
//    val unassign = dtoUnassignLegacy(
//      offset = offset(11),
//      eventSequentialId = 2L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      sourceSynchronizerId = someSynchronizerId2,
//      targetSynchronizerId = someSynchronizerId,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest the assign and unassign event
//    executeSql(
//      ingest(
//        Vector(
//          dtoPartyEntry(offset(1), signatoryParty),
//          dtoPartyEntry(offset(2), observerParty),
//          dtoPartyEntry(offset(3), nonStakeholderInformeeParty),
//        ) ++
//          Vector(
//            assign,
//            DbDto.IdFilterAssignStakeholder(
//              1L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(assign),
//            unassign,
//            DbDto.IdFilterUnassignStakeholder(
//              2L,
//              someTemplateId.toString,
//              signatoryParty,
//              first_per_sequential_id = true,
//            ),
//            metaFromSingle(unassign),
//          ),
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Vector[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(1)),
//      assignFilter = Vector(FilterAssign(1, 4)),
//      unassign = Vector(EventUnassign(2)),
//      unassignFilter = Vector(FilterUnassign(2, 4)),
//      txMeta = txMeta,
//    )
//
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(10), TxMeta(11))
//    )
//    // Prune at the offset of the assign event
//    pruneEventsSqlLegacy(offset(10))
//    assertAllDataPresent(
//      txMeta = Vector(TxMeta(11))
//    )
//    // Prune at the offset of the unassign event
//    pruneEventsSqlLegacy(offset(11))
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "not prune an assign" in {
//    def archive(
//        offsetInt: Int,
//        eventSequentialId: Long,
//        hashCidString: String = "#1",
//        synchronizerId: SynchronizerId = someSynchronizerId2,
//    ): Vector[DbDto] = {
//      val archive = dtoExerciseLegacy(
//        offset = offset(offsetInt.toLong),
//        eventSequentialId = eventSequentialId,
//        consuming = true,
//        contractId = hashCid(hashCidString),
//        signatory = signatoryParty,
//        synchronizerId = synchronizerId,
//      )
//      Vector(
//        archive,
//        DbDto
//          .IdFilterConsumingStakeholder(
//            eventSequentialId,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//        metaFromSingle(archive),
//      )
//    }
//    def unassign(
//        offsetInt: Int,
//        eventSequentialId: Long,
//        hashCidString: String = "#1",
//        synchronizerId: SynchronizerId = someSynchronizerId2,
//    ): Vector[DbDto] = {
//      val unassign = dtoUnassignLegacy(
//        offset = offset(offsetInt.toLong),
//        eventSequentialId = eventSequentialId,
//        contractId = hashCid(hashCidString),
//        signatory = signatoryParty,
//        sourceSynchronizerId = synchronizerId,
//        targetSynchronizerId = SynchronizerId.tryFromString("x::thirdsynchronizer"),
//      )
//      Vector(
//        unassign,
//        DbDto
//          .IdFilterUnassignStakeholder(
//            eventSequentialId,
//            someTemplateId.toString,
//            signatoryParty,
//            first_per_sequential_id = true,
//          ),
//        metaFromSingle(unassign),
//      )
//    }
//
//    val archiveDifferentSynchronizerEarlierThanAssing = archive(
//      offsetInt = 2,
//      eventSequentialId = 1,
//      synchronizerId = someSynchronizerId,
//    )
//    val unassignDifferentSynchronizerEarlierThanAssing = unassign(
//      offsetInt = 3,
//      eventSequentialId = 2,
//      synchronizerId = someSynchronizerId,
//    )
//    val unassignEarlierThanAssing = unassign(
//      offsetInt = 4,
//      eventSequentialId = 3,
//    )
//    val assign = dtoAssignLegacy(
//      offset = offset(5),
//      eventSequentialId = 4L,
//      contractId = hashCid("#1"),
//      signatory = signatoryParty,
//      sourceSynchronizerId = someSynchronizerId,
//      targetSynchronizerId = someSynchronizerId2,
//    )
//    val assignEvents = Vector(
//      assign,
//      DbDto.IdFilterAssignStakeholder(
//        4L,
//        someTemplateId.toString,
//        signatoryParty,
//        first_per_sequential_id = true,
//      ),
//      metaFromSingle(assign),
//    )
//    val archiveDifferentSynchronizerEarlierThanPruning = archive(
//      offsetInt = 6,
//      eventSequentialId = 5,
//      synchronizerId = someSynchronizerId,
//    )
//    val archiveDifferentCidEarlierThanPruning = archive(
//      offsetInt = 7,
//      eventSequentialId = 6,
//      hashCidString = "#2",
//    )
//    val unassignDifferentSynchronizerEarlierThanPruning = unassign(
//      offsetInt = 8,
//      eventSequentialId = 7,
//      synchronizerId = someSynchronizerId,
//    )
//    val unassignDifferentCidEarlierThanPruning = unassign(
//      offsetInt = 9, // pruning offset
//      eventSequentialId = 8,
//      hashCidString = "#2",
//    )
//    val archiveBeforeEnd = archive(
//      offsetInt = 10,
//      eventSequentialId = 9,
//    )
//    val unassignBeforeEnd = unassign(
//      offsetInt = 11,
//      eventSequentialId = 10,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest a create and archive event
//    executeSql(
//      ingest(
//        Vector(
//          Vector(dtoPartyEntry(offset(1), signatoryParty)),
//          archiveDifferentSynchronizerEarlierThanAssing,
//          unassignDifferentSynchronizerEarlierThanAssing,
//          unassignEarlierThanAssing,
//          assignEvents,
//          archiveDifferentSynchronizerEarlierThanPruning,
//          archiveDifferentCidEarlierThanPruning,
//          unassignDifferentSynchronizerEarlierThanPruning,
//          unassignDifferentCidEarlierThanPruning,
//          archiveBeforeEnd,
//          unassignBeforeEnd,
//        ).flatten,
//        _,
//      )
//    )
//
//    def assertAllDataPresent(txMeta: Seq[TxMeta]): Assertion = assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(4)),
//      assignFilter = Vector(FilterAssign(4, 6)),
//      consuming =
//        Vector(EventConsuming(1), EventConsuming(5), EventConsuming(6), EventConsuming(9)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(1, 6),
//        FilterConsumingStakeholder(5, 6),
//        FilterConsumingStakeholder(6, 6),
//        FilterConsumingStakeholder(9, 6),
//      ),
//      unassign = Vector(
//        EventUnassign(2),
//        EventUnassign(3),
//        EventUnassign(7),
//        EventUnassign(8),
//        EventUnassign(10),
//      ),
//      unassignFilter = Vector(
//        FilterUnassign(2, 6),
//        FilterUnassign(3, 6),
//        FilterUnassign(7, 6),
//        FilterUnassign(8, 6),
//        FilterUnassign(10, 6),
//      ),
//      txMeta = txMeta,
//    )
//    assertAllDataPresent(
//      txMeta = Vector(
//        TxMeta(2),
//        TxMeta(3),
//        TxMeta(4),
//        TxMeta(5),
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//        TxMeta(9),
//        TxMeta(10),
//        TxMeta(11),
//      )
//    )
//    // Prune earlier
//    pruneEventsSqlLegacy(offset(1))
//    assertAllDataPresent(
//      txMeta = Vector(
//        TxMeta(2),
//        TxMeta(3),
//        TxMeta(4),
//        TxMeta(5),
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//        TxMeta(9),
//        TxMeta(10),
//        TxMeta(11),
//      )
//    )
//    // Prune at assign
//    pruneEventsSqlLegacy(offset(5))
//    assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(4)),
//      assignFilter = Vector(FilterAssign(4, 6)),
//      consuming = Vector(EventConsuming(5), EventConsuming(6), EventConsuming(9)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(5, 6),
//        FilterConsumingStakeholder(6, 6),
//        FilterConsumingStakeholder(9, 6),
//      ),
//      unassign = Vector(
//        EventUnassign(7),
//        EventUnassign(8),
//        EventUnassign(10),
//      ),
//      unassignFilter = Vector(
//        FilterUnassign(7, 6),
//        FilterUnassign(8, 6),
//        FilterUnassign(10, 6),
//      ),
//      txMeta = Vector(
//        TxMeta(6),
//        TxMeta(7),
//        TxMeta(8),
//        TxMeta(9),
//        TxMeta(10),
//        TxMeta(11),
//      ),
//    )
//    // Prune after unrelated archive and reassign events but before related ones
//    pruneEventsSqlLegacy(offset(9))
//    assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(4)),
//      assignFilter = Vector(FilterAssign(4, 6)),
//      consuming = Vector(EventConsuming(9)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(9, 6)
//      ),
//      unassign = Vector(
//        EventUnassign(10)
//      ),
//      unassignFilter = Vector(
//        FilterUnassign(10, 6)
//      ),
//      txMeta = Vector(
//        TxMeta(10),
//        TxMeta(11),
//      ),
//    )
//    // Prune at the end, but with setting the assign incomplete
//    // (the archive and the unassing cannot be pruned neither, because they belong to an incomplete activation)
//    pruneEventsSqlLegacy(
//      offset(11),
//      Vector(offset(5), offset(1000), offset(1001)),
//    )
//    assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(4)),
//      assignFilter = Vector(FilterAssign(4, 6)),
//      consuming = Vector(EventConsuming(9)),
//      consumingFilterStakeholder = Vector(
//        FilterConsumingStakeholder(9, 6)
//      ),
//      unassign = Vector(
//        EventUnassign(10)
//      ),
//      unassignFilter = Vector(
//        FilterUnassign(10, 6)
//      ),
//      txMeta = Vector.empty,
//    )
//    // Prune at the end, but with setting the following unassign incomplete
//    // (the following archive can be pruned, but the following unassign and the assign can't, to be able to look up create event for the incomplete unassigned)
//    pruneEventsSqlLegacy(
//      offset(11),
//      Vector(offset(11), offset(1000), offset(1001)),
//    )
//    assertIndexDbDataSqlLegacy(
//      assign = Vector(EventAssign(4)),
//      assignFilter = Vector(FilterAssign(4, 6)),
//      unassign = Vector(
//        EventUnassign(10)
//      ),
//      unassignFilter = Vector(
//        FilterUnassign(10, 6)
//      ),
//      txMeta = Vector.empty,
//    )
//    // Prune at the end
//    pruneEventsSqlLegacy(offset(11))
//    assertIndexDbDataSqlLegacy()
//  }
//
//  it should "prune all retroactively and immediately divulged contracts" in {
//    val partyName = "party"
//    val divulgee = Ref.Party.assertFromString(partyName)
//    val contract1_id = hashCid("#1")
//    val contract2_id = hashCid("#2")
//    val contract1_immediateDivulgence = dtoCreateLegacy(
//      offset = offset(1),
//      eventSequentialId = 1L,
//      contractId = contract1_id,
//      signatory = divulgee,
//      emptyFlatEventWitnesses = true,
//    )
//    val contract2_createWithLocalStakeholder = dtoCreateLegacy(
//      offset = offset(2),
//      eventSequentialId = 2L,
//      contractId = contract2_id,
//      signatory = divulgee,
//    )
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    // Ingest
//    executeSql(
//      ingest(
//        Vector(
//          contract1_immediateDivulgence,
//          DbDto.IdFilterCreateNonStakeholderInformee(
//            1L,
//            someTemplateId.toString,
//            divulgee,
//            first_per_sequential_id = true,
//          ),
//          contract2_createWithLocalStakeholder,
//          DbDto.IdFilterCreateStakeholder(
//            2L,
//            someTemplateId.toString,
//            divulgee,
//            first_per_sequential_id = true,
//          ),
//        ),
//        _,
//      )
//    )
//    assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(1), EventCreate(2)),
//      createFilterNonStakeholder = Vector(FilterCreateNonStakeholder(1, 3)),
//      createFilterStakeholder = Vector(FilterCreateStakeholder(2, 3)),
//    )
//    pruneEventsSqlLegacy(offset(2))
//    assertIndexDbDataSqlLegacy(
//      create = Vector(EventCreate(2)),
//      createFilterStakeholder = Vector(FilterCreateStakeholder(2, 3)),
//    )
//  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitters = Set(someParty),
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a completion
    executeSql(ingest(Vector(completion), _))
    assertIndexDbDataSql(completion = Seq(PruningDto.Completion(1)))
    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, TraceContext.empty))
    assertIndexDbDataSql(completion = Seq.empty)
  }

  it should "prune various witnessed events" in {
    val updates = Vector(
      // before pruning start
      meta(event_offset = 1)(
        dtosWitnessedCreate(
          event_sequential_id = 100
        )()
      ),
      meta(event_offset = 2)(
        dtosWitnessedExercised(
          event_sequential_id = 200,
          consuming = true,
        )
      ),
      meta(event_offset = 3)(
        dtosWitnessedExercised(
          event_sequential_id = 300,
          consuming = false,
        )
      ),
      // in pruning range
      meta(event_offset = 4)(
        dtosWitnessedCreate(
          event_sequential_id = 400
        )()
      ),
      meta(event_offset = 5)(
        dtosWitnessedExercised(
          event_sequential_id = 500,
          consuming = true,
        )
      ),
      meta(event_offset = 6)(
        dtosWitnessedExercised(
          event_sequential_id = 600,
          consuming = false,
        )
      ),
      // after pruning range
      meta(event_offset = 7)(
        dtosWitnessedCreate(
          event_sequential_id = 700
        )()
      ),
      meta(event_offset = 8)(
        dtosWitnessedExercised(
          event_sequential_id = 800,
          consuming = true,
        )
      ),
      meta(event_offset = 9)(
        dtosWitnessedExercised(
          event_sequential_id = 900,
          consuming = false,
        )
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(9), 900L))
    assertIndexDbDataSql(
      variousWitnessed = List(
        100, 200, 300, 400, 500, 600, 700, 800, 900,
      ),
      variousFilterWitness = List(
        100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(5),
        TxMeta(6),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(3)),
      previousIncompleteReassignmentOffsets = Vector.empty,
      pruneUpToInclusive = offset(6),
      incompleteReassignmentOffsets = Vector.empty,
    )(TraceContext.empty)

    assertIndexDbDataSql(
      variousWitnessed = List(
        100, 200, 300, 700, 800, 900,
      ),
      variousFilterWitness = List(
        100, 100, 200, 200, 300, 300, 700, 700, 800, 800, 900, 900,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
      ),
    )
  }

  it should "prune activate and deactivate events" in {
    val updates = Vector(
      // before pruning start will be pruned later
      meta(event_offset = 1)(
        dtosCreate(
          event_sequential_id = 100
        )()
      ),
      meta(event_offset = 2)(
        dtosAssign(
          event_sequential_id = 200
        )()
      ),
      // before pruning start won't be pruned later
      meta(event_offset = 3)(
        dtosCreate(
          event_sequential_id = 300
        )()
      ),
      meta(event_offset = 4)(
        dtosAssign(
          event_sequential_id = 400
        )()
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(
          event_sequential_id = 500
        )()
      ),
      meta(event_offset = 6)(
        dtosAssign(
          event_sequential_id = 600
        )()
      ),
      // in pruning range will be not pruned - no deactivation
      meta(event_offset = 7)(
        dtosCreate(
          event_sequential_id = 700
        )()
      ),
      meta(event_offset = 8)(
        dtosAssign(
          event_sequential_id = 800
        )()
      ),
      // in pruning range will be not pruned - deactivation outside of the pruning range
      meta(event_offset = 9)(
        dtosCreate(
          event_sequential_id = 900
        )()
      ),
      meta(event_offset = 10)(
        dtosAssign(
          event_sequential_id = 1000
        )()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(100),
        )
      ),
      meta(event_offset = 12)(
        dtosUnassign(
          event_sequential_id = 1200,
          deactivated_event_sequential_id = Some(200),
        )
      ),
      meta(event_offset = 13)(
        dtosUnassign(
          event_sequential_id = 1300,
          deactivated_event_sequential_id = Some(500),
        )
      ),
      meta(event_offset = 14)(
        dtosConsumingExercise(
          event_sequential_id = 1400,
          deactivated_event_sequential_id = Some(600),
        )
      ),
      meta(event_offset = 15)(
        dtosConsumingExercise(
          event_sequential_id = 1500,
          deactivated_event_sequential_id = None,
        )
      ),
      // outside of pruning range some activations deactivated later
      meta(event_offset = 16)(
        dtosCreate(
          event_sequential_id = 1600
        )()
      ),
      meta(event_offset = 17)(
        dtosAssign(
          event_sequential_id = 1700
        )()
      ),
      // outside of pruning range some activations never deactivated
      meta(event_offset = 18)(
        dtosCreate(
          event_sequential_id = 1800
        )()
      ),
      meta(event_offset = 19)(
        dtosAssign(
          event_sequential_id = 1900
        )()
      ),
      // outside of pruning range some deactivations
      meta(event_offset = 20)(
        dtosUnassign(
          event_sequential_id = 2000,
          deactivated_event_sequential_id = Some(1700),
        )
      ),
      meta(event_offset = 21)(
        dtosConsumingExercise(
          event_sequential_id = 2100,
          deactivated_event_sequential_id = Some(1600),
        )
      ),
      meta(event_offset = 22)(
        dtosConsumingExercise(
          event_sequential_id = 2200,
          deactivated_event_sequential_id = None,
        )
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1600, 1700, 1800, 1900,
      ),
      activateFilterStakeholder = List(
        100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900,
        1000, 1000, 1600, 1600, 1700, 1700, 1800, 1800, 1900, 1900,
      ),
      activateFilterWitness = List(
        100, 100, 300, 300, 500, 500, 700, 700, 900, 900, 1600, 1600, 1800, 1800,
      ),
      deactivate = List(
        1100, 1200, 1300, 1400, 1500, 2000, 2100, 2200,
      ),
      deactivateFilterStakeholder = List(
        1100, 1100, 1200, 1200, 1300, 1300, 1400, 1400, 1500, 1500, 2000, 2000, 2100, 2100, 2200,
        2200,
      ),
      deactivateFilterWitness = List(
        1100, 1100, 1400, 1400, 1500, 1500, 2100, 2100, 2200, 2200,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(5),
        TxMeta(6),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
        TxMeta(10),
        TxMeta(11),
        TxMeta(12),
        TxMeta(13),
        TxMeta(14),
        TxMeta(15),
        TxMeta(16),
        TxMeta(17),
        TxMeta(18),
        TxMeta(19),
        TxMeta(20),
        TxMeta(21),
        TxMeta(22),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector.empty,
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector.empty,
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        300, 400, 700, 800, 900, 1000, 1600, 1700, 1800, 1900,
      ),
      activateFilterStakeholder = List(
        300, 300, 400, 400, 700, 700, 800, 800, 900, 900, 1000, 1000, 1600, 1600, 1700, 1700, 1800,
        1800, 1900, 1900,
      ),
      activateFilterWitness = List(
        300, 300, 700, 700, 900, 900, 1600, 1600, 1800, 1800,
      ),
      deactivate = List(
        2000,
        2100,
        2200,
      ),
      deactivateFilterStakeholder = List(
        2000, 2000, 2100, 2100, 2200, 2200,
      ),
      deactivateFilterWitness = List(
        2100,
        2100,
        2200,
        2200,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(16),
        TxMeta(17),
        TxMeta(18),
        TxMeta(19),
        TxMeta(20),
        TxMeta(21),
        TxMeta(22),
      ),
    )
  }

  it should "not prune incomplete events and related other events, but prune completed, older incomplete events and related other events" in {
    val updates = Vector(
      // before pruning start, incomplete assignments, won't be pruned
      meta(event_offset = 2)(
        dtosAssign(event_sequential_id = 200)() ++
          dtosAssign(event_sequential_id = 201)() ++
          dtosAssign(event_sequential_id = 202)()
      ),
      // before pruning start, relates to incomplete, so still retained
      meta(event_offset = 3)(
        dtosConsumingExercise(
          event_sequential_id = 300,
          deactivated_event_sequential_id = Some(202),
        )
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(event_sequential_id = 500)()
      ),
      meta(event_offset = 6)(
        dtosCreate(event_sequential_id = 600)()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(500),
        ) ++
          // related to incomplete assignment, won't be pruned
          dtosConsumingExercise(
            event_sequential_id = 1101,
            deactivated_event_sequential_id = Some(201),
          )
      ),
      // incomplete unassignments, won't be pruned
      meta(event_offset = 12)(
        // this also relates to an incomplete unassignment
        dtosUnassign(event_sequential_id = 1200, deactivated_event_sequential_id = Some(200)) ++
          dtosUnassign(event_sequential_id = 1201, deactivated_event_sequential_id = Some(600))
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        200, 201, 202, 500, 600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 500, 500, 600, 600,
      ),
      activateFilterWitness = List(
        500,
        500,
        600,
        600,
      ),
      deactivate = List(
        300, 1100, 1101, 1200, 1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1100, 1100, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300, 300, 1100, 1100, 1101, 1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
        TxMeta(5),
        TxMeta(6),
        TxMeta(11),
        TxMeta(12),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector(offset(2), offset(12)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        201,
        202,
        600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 600, 600,
      ),
      activateFilterWitness = List(
        600,
        600,
      ),
      deactivate = List(
        300,
        1101,
        1200,
        1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300,
        300,
        1101,
        1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(15)),
      previousIncompleteReassignmentOffsets = Vector(offset(2), offset(12)),
      pruneUpToInclusive = offset(17),
      incompleteReassignmentOffsets = Vector(offset(2)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        201,
        202,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202,
      ),
      activateFilterWitness = List(),
      deactivate = List(
        300,
        1101,
        1200,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1101, 1101, 1200, 1200,
      ),
      deactivateFilterWitness = List(
        300,
        300,
        1101,
        1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(17)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(21),
      incompleteReassignmentOffsets = Vector(),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(),
      activateFilterStakeholder = List(),
      activateFilterWitness = List(),
      deactivate = List(),
      deactivateFilterStakeholder = List(),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )
  }

  it should "not prune incomplete events and related other events, but prune completed, older incomplete events and related other events - combined case having new incomplete and complete as well" in {
    val updates = Vector(
      // before pruning start, older incomplete assignments, will become completed, and prunable
      meta(event_offset = 2)(
        dtosAssign(event_sequential_id = 200)() ++
          dtosAssign(event_sequential_id = 201)() ++
          dtosAssign(event_sequential_id = 202)()
      ),
      // before pruning start, relates to incomplete, so still retained previously, but with that becoming completed, prunable
      meta(event_offset = 3)(
        dtosConsumingExercise(
          event_sequential_id = 300,
          deactivated_event_sequential_id = Some(202),
        )
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(event_sequential_id = 500)()
      ),
      meta(event_offset = 6)(
        dtosCreate(event_sequential_id = 600)()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(500),
        ) ++
          // related to completed assignment, will be pruned
          dtosConsumingExercise(
            event_sequential_id = 1101,
            deactivated_event_sequential_id = Some(201),
          )
      ),
      // incomplete unassignments, won't be pruned
      meta(event_offset = 12)(
        // this also relates to a previously incomplete unassignment
        dtosUnassign(event_sequential_id = 1200, deactivated_event_sequential_id = Some(200)) ++
          dtosUnassign(event_sequential_id = 1201, deactivated_event_sequential_id = Some(600))
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        200, 201, 202, 500, 600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 500, 500, 600, 600,
      ),
      activateFilterWitness = List(
        500,
        500,
        600,
        600,
      ),
      deactivate = List(
        300, 1100, 1101, 1200, 1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1100, 1100, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300, 300, 1100, 1100, 1101, 1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
        TxMeta(5),
        TxMeta(6),
        TxMeta(11),
        TxMeta(12),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector(offset(12)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        600,
      ),
      activateFilterStakeholder = List(
        200,
        200,
        600,
        600,
      ),
      activateFilterWitness = List(
        600,
        600,
      ),
      deactivate = List(
        1200,
        1201,
      ),
      deactivateFilterStakeholder = List(
        1200,
        1200,
        1201,
        1201,
      ),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(15)),
      previousIncompleteReassignmentOffsets = Vector(offset(12)),
      pruneUpToInclusive = offset(17),
      incompleteReassignmentOffsets = Vector(),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(),
      activateFilterStakeholder = List(),
      activateFilterWitness = List(),
      deactivate = List(),
      deactivateFilterStakeholder = List(),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )
  }

  // TODO(i21351) Implement pruning tests for topology events

  /** Asserts the content of the tables subject to pruning. Be default asserts the tables are empty.
    */
  def assertIndexDbDataSql(
      activate: Seq[Long] = Seq.empty,
      activateFilterStakeholder: Seq[Long] = Seq.empty,
      activateFilterWitness: Seq[Long] = Seq.empty,
      deactivate: Seq[Long] = Seq.empty,
      deactivateFilterStakeholder: Seq[Long] = Seq.empty,
      deactivateFilterWitness: Seq[Long] = Seq.empty,
      variousWitnessed: Seq[Long] = Seq.empty,
      variousFilterWitness: Seq[Long] = Seq.empty,
      txMeta: Seq[TxMeta] = Seq.empty,
      completion: Seq[Completion] = Seq.empty,
  ): Assertion = executeSql { implicit c =>
    val queries = backend.pruningDtoQueries
    val cp = new Checkpoint
    // activate
    cp(clue("activate")(Statement.discard(queries.eventActivate shouldBe activate)))
    cp(
      clue("activate filter stakeholder")(
        Statement.discard(queries.filterActivateStakeholder shouldBe activateFilterStakeholder)
      )
    )
    cp(
      clue("activate filter witness")(
        Statement.discard(queries.filterActivateWitness shouldBe activateFilterWitness)
      )
    )
    // deactivate
    cp(clue("deactivate")(Statement.discard(queries.eventDeactivate shouldBe deactivate)))
    cp(
      clue("deactivate filter stakeholder")(
        Statement.discard(queries.filterDeactivateStakeholder shouldBe deactivateFilterStakeholder)
      )
    )
    cp(
      clue("deactivate filter witness")(
        Statement.discard(queries.filterDeactivateWitness shouldBe deactivateFilterWitness)
      )
    )
    // witnessed
    cp(
      clue("various witnessed")(
        Statement.discard(queries.eventVariousWitnessed shouldBe variousWitnessed)
      )
    )
    cp(
      clue("various witnessed filter")(
        Statement.discard(queries.filterVariousWitness shouldBe variousFilterWitness)
      )
    )
    // other
    cp(clue("meta")(Statement.discard(queries.updateMeta shouldBe txMeta)))
    cp(clue("completion")(Statement.discard(queries.completions shouldBe completion)))
    cp.reportAll()
    succeed
  }
}
