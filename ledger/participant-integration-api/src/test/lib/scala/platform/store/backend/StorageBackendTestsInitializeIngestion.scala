// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import org.scalatest.compatible.Assertion
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitializeIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initializeIngestion)"

  import StorageBackendTestValues._

  private def dtoMetering(app: String, offset: Offset) =
    dtoTransactionMetering(
      TransactionMetering(Ref.ApplicationId.assertFromString(app), 1, someTime, offset)
    )

  private val signatory = Ref.Party.assertFromString("signatory")
  private val readers = Set(signatory)

  {
    val dtos = Vector(
      // 1: config change
      dtoConfiguration(offset(1), someConfiguration),
      // 2: party allocation
      dtoPartyEntry(offset(2), "party1"),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
    )
    it should "delete overspill entries - config, parties, packages" in {
      fixture(
        dtos1 = dtos,
        lastOffset1 = 3L,
        lastEventSeqId1 = 0L,
        dtos2 = Vector(
          // 4: config change
          dtoConfiguration(offset(4), someConfiguration),
          // 5: party allocation
          dtoPartyEntry(offset(5), "party2"),
          // 6: package upload
          dtoPackage(offset(6)),
          dtoPackageEntry(offset(6)),
        ),
        lastOffset2 = 6L,
        lastEventSeqId2 = 0L,
        checkContentsBefore = () => {
          val parties = executeSql(backend.party.knownParties)
          val config = executeSql(backend.configuration.ledgerConfiguration)
          val packages = executeSql(backend.packageBackend.lfPackages)
          parties should have length 1
          packages should have size 1
          config shouldBe Some(offset(1) -> someConfiguration)
        },
        checkContentsAfter = () => {
          val parties = executeSql(backend.party.knownParties)
          val config = executeSql(backend.configuration.ledgerConfiguration)
          val packages = executeSql(backend.packageBackend.lfPackages)
          parties should have length 1
          packages should have size 1
          config shouldBe Some(offset(1) -> someConfiguration)
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - config, parties, packages" in {
      fixtureOverspillBeforeFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 3,
        lastEventSeqId = 0L,
        checkContentsAfter = () => {
          val parties2 = executeSql(backend.party.knownParties)
          val config2 = executeSql(backend.configuration.ledgerConfiguration)
          val packages2 = executeSql(backend.packageBackend.lfPackages)
          parties2 shouldBe empty
          packages2 shouldBe empty
          config2 shouldBe empty
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
      fixtureOverspillBeforeFirstLedgerEndUpdate(
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
      // 2: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(2), 2L, false, hashCid("#101")),
      DbDto.IdFilterNonConsumingInformee(2L, someParty),
      dtoExercise(offset(2), 3L, true, hashCid("#102")),
      DbDto.IdFilterConsumingStakeholder(3L, someTemplateId.toString, someParty),
      DbDto.IdFilterConsumingNonStakeholderInformee(3L, someParty),
      dtoDivulgence(Some(offset(2)), 4L, hashCid("#101")),
      dtoTransactionMeta(
        offset(2),
        event_sequential_id_first = 2L,
        event_sequential_id_last = 4L,
      ),
      dtoCompletion(offset(2)),
    )

    it should "delete overspill entries - events, transaction meta, completions" in {
      fixture(
        dtos1 = dtos,
        lastOffset1 = 2L,
        lastEventSeqId1 = 4L,
        dtos2 = Vector(
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
          // 4: transaction with exercise node and retroactive divulgence
          dtoExercise(offset(4), 6L, false, hashCid("#201")),
          DbDto.IdFilterNonConsumingInformee(6L, someParty),
          dtoExercise(offset(4), 7L, true, hashCid("#202")),
          DbDto.IdFilterConsumingStakeholder(7L, someTemplateId.toString, someParty),
          DbDto.IdFilterConsumingNonStakeholderInformee(7L, someParty),
          dtoDivulgence(Some(offset(4)), 8L, hashCid("#201")),
          dtoTransactionMeta(
            offset(4),
            event_sequential_id_first = 6L,
            event_sequential_id_last = 8L,
          ),
          dtoCompletion(offset(4)),
        ),
        lastOffset2 = 10L,
        lastEventSeqId2 = 6L,
        checkContentsBefore = () => {
          val contract101 =
            executeSql(backend.contract.activeContractWithoutArgument(readers, hashCid("#101")))
          val contract202 =
            executeSql(backend.contract.activeContractWithoutArgument(readers, hashCid("#201")))
          // TODO etq: Add assertion for the remaining filter tables & transaction_meta table
          val idsCreateStakeholder = executeSql(
            backend.event.activeContractEventIds(
              partyFilter = someParty,
              templateIdFilter = None,
              startExclusive = 0,
              endInclusive = 1000,
              limit = 1000,
            )
          )
          contract101 should not be empty
          contract202 shouldBe None
          idsCreateStakeholder shouldBe List(
            1L,
            5L,
          ) // since ledger-end does not limit the range query
        },
        checkContentsAfter = () => {
          val contract101 = executeSql(
            backend.contract.activeContractWithoutArgument(readers, hashCid("#101"))
          )
          val contract202 = executeSql(
            backend.contract.activeContractWithoutArgument(readers, hashCid("#201"))
          )
          // TODO etq: Add assertion for the remaining filter tables & transaction_meta table
          val idsCreateStakeholder = executeSql(
            backend.event.activeContractEventIds(
              partyFilter = someParty,
              templateIdFilter = None,
              startExclusive = 0,
              endInclusive = 1000,
              limit = 1000,
            )
          )
          contract101 should not be empty
          contract202 shouldBe None
          idsCreateStakeholder shouldBe List(1L)
        },
      )
    }

    it should "delete overspill entries written before first ledger end update - events, transaction meta, completions" in {
      fixtureOverspillBeforeFirstLedgerEndUpdate(
        dtos = dtos,
        lastOffset = 2,
        lastEventSeqId = 3L,
        checkContentsAfter = () => {
          val contract101 = executeSql(
            backend.contract.activeContractWithoutArgument(readers, hashCid("#101"))
          )
          // TODO etq: Add assertion for the remaining filter tables & transaction_meta table
          val idsCreateStakeholder = executeSql(
            backend.event.activeContractEventIds(
              partyFilter = someParty,
              templateIdFilter = None,
              startExclusive = 0,
              endInclusive = 1000,
              limit = 1000,
            )
          )
          contract101 shouldBe None
          idsCreateStakeholder shouldBe empty
        },
      )
    }
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
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd))
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

  private def fixtureOverspillBeforeFirstLedgerEndUpdate(
      dtos: Vector[DbDto],
      lastOffset: Long,
      lastEventSeqId: Long,
      checkContentsAfter: () => Assertion,
  ): Assertion = {
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd))
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
