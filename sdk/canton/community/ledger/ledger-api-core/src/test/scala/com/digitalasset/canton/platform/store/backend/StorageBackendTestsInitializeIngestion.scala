// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.common.EventIdSourceForInformees
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
      TransactionMetering(Ref.ApplicationId.assertFromString(app), 1, someTime, offset)
    )

  private val signatory = Ref.Party.assertFromString("signatory")
  private val readers = Set(signatory)

  {
    val dtos = Vector(
      // 1: party allocation
      dtoPartyEntry(offset(1), "party1"),
      // 2: package upload
      dtoPackage(offset(2)),
      dtoPackageEntry(offset(2)),
    )
    it should "delete overspill entries - parties, packages" in {
      fixture(
        dtos1 = dtos,
        lastOffset1 = 2L,
        lastEventSeqId1 = 0L,
        dtos2 = Vector(
          // 3: party allocation
          dtoPartyEntry(offset(3), "party2"),
          // 4: package upload
          dtoPackage(offset(4)),
          dtoPackageEntry(offset(4)),
        ),
        lastOffset2 = 4L,
        lastEventSeqId2 = 0L,
        checkContentsBefore = () => {
          val parties = executeSql(backend.party.knownParties(None, 10))
          val packages = executeSql(backend.packageBackend.lfPackages)
          parties should have length 1
          packages should have size 1
        },
        checkContentsAfter = () => {
          val parties = executeSql(backend.party.knownParties(None, 10))
          val packages = executeSql(backend.packageBackend.lfPackages)
          parties should have length 1
          packages should have size 1
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - parties, packages" in {
      fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 3,
        lastEventSeqId = 0L,
        checkContentsAfter = () => {
          val parties2 = executeSql(backend.party.knownParties(None, 10))
          val packages2 = executeSql(backend.packageBackend.lfPackages)
          parties2 shouldBe empty
          packages2 shouldBe empty
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
      DbDto.IdFilterCreateNonStakeholderInformee(1L, someParty),
      dtoTransactionMeta(
        offset(1),
        event_sequential_id_first = 1L,
        event_sequential_id_last = 1L,
      ),
      dtoCompletion(offset(41)),
      // 2: transaction with exercise node
      dtoExercise(offset(2), 2L, false, hashCid("#101")),
      DbDto.IdFilterNonConsumingInformee(2L, someParty),
      dtoExercise(offset(2), 3L, true, hashCid("#102")),
      DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, someParty),
      DbDto.IdFilterConsumingNonStakeholderInformee(3L, someParty),
      dtoTransactionMeta(
        offset(2),
        event_sequential_id_first = 2L,
        event_sequential_id_last = 4L,
      ),
      dtoCompletion(offset(2)),
    )

    it should "delete overspill entries - events, transaction meta, completions" in {
      val dtos2 = Vector(
        // 3: transaction with create node
        dtoCreate(offset(3), 5L, hashCid("#201"), signatory = signatory),
        DbDto.IdFilterCreateStakeholder(5L, someTemplateId.toString, someParty),
        DbDto.IdFilterCreateNonStakeholderInformee(5L, someParty),
        dtoTransactionMeta(
          offset(3),
          event_sequential_id_first = 5L,
          event_sequential_id_last = 5L,
        ),
        dtoCompletion(offset(3)),
        // 4: transaction with exercise node
        dtoExercise(offset(4), 6L, false, hashCid("#201")),
        DbDto.IdFilterNonConsumingInformee(6L, someParty),
        dtoExercise(offset(4), 7L, true, hashCid("#202")),
        DbDto.IdFilterConsumingStakeholder(7L, someTemplateId.toString, someParty),
        DbDto.IdFilterConsumingNonStakeholderInformee(7L, someParty),
        dtoTransactionMeta(
          offset(4),
          event_sequential_id_first = 6L,
          event_sequential_id_last = 8L,
        ),
        dtoCompletion(offset(4)),
      )
      val allDtos = dtos ++ dtos2
      fixture(
        dtos1 = dtos,
        lastOffset1 = 2L,
        lastEventSeqId1 = 4L,
        dtos2 = dtos2,
        lastOffset2 = 10L,
        lastEventSeqId2 = 6L,
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
          contractsCreated.get(hashCid("#101")) should not be empty
          contractsCreated.get(hashCid("#201")) should not be empty
          contractsArchived.get(hashCid("#101")) shouldBe empty
          contractsArchived.get(hashCid("#201")) shouldBe empty
          fetchIdsFromTransactionMeta(allDtos.collect { case meta: DbDto.TransactionMeta =>
            meta.transaction_id
          }) shouldBe Set((1, 1), (2, 4))
          fetchIdsCreateStakeholder() shouldBe List(
            1L,
            5L,
          ) // since ledger-end does not limit the range query
          fetchIdsCreateNonStakeholder() shouldBe List(1L, 5L)
          fetchIdsConsumingStakeholder() shouldBe List(3L, 7L)
          fetchIdsConsumingNonStakeholder() shouldBe List(3L, 7L)
          fetchIdsNonConsuming() shouldBe List(2L, 6L)
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
          contractsCreated.get(hashCid("#101")) should not be empty
          contractsCreated.get(hashCid("#201")) shouldBe empty
          contractsArchived.get(hashCid("#101")) shouldBe empty
          contractsArchived.get(hashCid("#201")) shouldBe empty
          fetchIdsFromTransactionMeta(allDtos.collect { case meta: DbDto.TransactionMeta =>
            meta.transaction_id
          }) shouldBe Set((1, 1), (2, 4))
          fetchIdsCreateStakeholder() shouldBe List(1L)
          fetchIdsCreateNonStakeholder() shouldBe List(1L)
          fetchIdsConsumingStakeholder() shouldBe List(3L)
          fetchIdsConsumingNonStakeholder() shouldBe List(3L)
          fetchIdsNonConsuming() shouldBe List(2L)
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - events, transaction meta, completions" in {
      fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 2,
        lastEventSeqId = 3L,
        checkContentsAfter = () => {
          val contractsCreated =
            executeSql(
              backend.contract
                .createdContracts(List(hashCid("#101"), hashCid("#201")), offset(1000))
            )
          contractsCreated.get(hashCid("#101")) shouldBe None
          fetchIdsFromTransactionMeta(dtos.collect { case meta: DbDto.TransactionMeta =>
            meta.transaction_id
          }) shouldBe empty
          fetchIdsCreateStakeholder() shouldBe empty
          fetchIdsCreateNonStakeholder() shouldBe empty
          fetchIdsConsumingStakeholder() shouldBe empty
          fetchIdsConsumingNonStakeholder() shouldBe empty
          fetchIdsNonConsuming() shouldBe empty
        },
      )
    }
  }

  private def fetchIdsNonConsuming(): Vector[Long] = {
    executeSql(
      backend.event.transactionStreamingQueries.fetchEventIdsForInformee(
        EventIdSourceForInformees.NonConsumingInformee
      )(informee = someParty, startExclusive = 0, endInclusive = 1000, limit = 1000)
    )
  }

  private def fetchIdsConsumingNonStakeholder(): Vector[Long] = {
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIdsForInformee(EventIdSourceForInformees.ConsumingNonStakeholder)(
          informee = someParty,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )
  }

  private def fetchIdsConsumingStakeholder(): Vector[Long] = {
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIdsForInformee(EventIdSourceForInformees.ConsumingStakeholder)(
          informee = someParty,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )
  }

  private def fetchIdsCreateNonStakeholder(): Vector[Long] = {
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIdsForInformee(EventIdSourceForInformees.CreateNonStakeholder)(
          informee = someParty,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )
  }

  private def fetchIdsCreateStakeholder(): Vector[Long] = {
    executeSql(
      backend.event.transactionStreamingQueries
        .fetchEventIdsForInformee(EventIdSourceForInformees.CreateStakeholder)(
          informee = someParty,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
    )
  }

  private def fetchIdsFromTransactionMeta(transactionIds: Seq[String]): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.transactionPointwiseQueries
    transactionIds
      .map(Ref.TransactionId.assertFromString)
      .map { transactionId =>
        executeSql(txPointwiseQueries.fetchIdsFromTransactionMeta(transactionId))
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
