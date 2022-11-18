// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
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

  it should "delete overspill entries" in {
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos1: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1), someConfiguration),
      // 2: party allocation
      dtoPartyEntry(offset(2), "party1"),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
      // 4: transaction with create node
      dtoCreate(offset(4), 1L, hashCid("#4"), signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, false, hashCid("#4")),
      dtoDivulgence(Some(offset(5)), 3L, hashCid("#4")),
      dtoCompletion(offset(5)),
      // Transaction Metering
      dtoMetering("AppA", offset(1)),
      dtoMetering("AppB", offset(4)),
    )

    val dtos2: Vector[DbDto] = Vector(
      // 6: config change
      dtoConfiguration(offset(6), someConfiguration),
      // 7: party allocation
      dtoPartyEntry(offset(7), "party2"),
      // 8: package upload
      dtoPackage(offset(8)),
      dtoPackageEntry(offset(8)),
      // 9: transaction with create node
      dtoCreate(offset(9), 4L, hashCid("#9"), signatory = signatory),
      DbDto.IdFilterCreateStakeholder(4L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(9)),
      // 10: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(10), 5L, false, hashCid("#9")),
      dtoDivulgence(Some(offset(10)), 6L, hashCid("#9")),
      dtoCompletion(offset(10)),
      // Transaction Metering
      dtoMetering("AppC", offset(6)),
    )

    val readers = Set(signatory)

    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd))

    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))

    // Fully insert first batch of updates
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(ledgerEnd(5, 3L)))

    // Partially insert second batch of updates (indexer crashes before updating ledger end)
    executeSql(ingest(dtos2, _))

    // Check the contents
    val parties1 = executeSql(backend.party.knownParties)
    val config1 = executeSql(backend.configuration.ledgerConfiguration)
    val packages1 = executeSql(backend.packageBackend.lfPackages)
    val contract41 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#4"),
      )
    )
    val contract91 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#9"),
      )
    )
    val filterIds1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholders(
        stakeholder = someParty,
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    val metering1 =
      executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))

    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))

    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(10, 6L)))

    // Check the contents
    val parties2 = executeSql(backend.party.knownParties)
    val config2 = executeSql(backend.configuration.ledgerConfiguration)
    val packages2 = executeSql(backend.packageBackend.lfPackages)
    val contract42 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#4"),
      )
    )
    val contract92 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#9"),
      )
    )
    val filterIds2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholders(
        stakeholder = someParty,
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    val metering2 =
      executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))

    parties1 should have length 1
    packages1 should have size 1
    config1 shouldBe Some(offset(1) -> someConfiguration)
    contract41 should not be empty
    contract91 shouldBe None
    filterIds1 shouldBe List(1L, 4L) // since ledger-end does not limit the range query

    // Metering report can include partially ingested data in non-final reports
    metering1.applicationData should have size 3
    metering1.isFinal shouldBe false

    parties2 should have length 1
    packages2 should have size 1
    config2 shouldBe Some(offset(1) -> someConfiguration)
    contract42 should not be empty
    contract92 shouldBe None
    filterIds2 shouldBe List(1L)

    metering2.applicationData should have size 2 // Partially ingested data removed

  }

  it should "delete overspill entries written before first ledger end update" in {
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos1: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1), someConfiguration),
      // 2: party allocation
      dtoPartyEntry(offset(2), "party1"),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
      // 4: transaction with create node
      dtoCreate(offset(4), 1L, hashCid("#4"), signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, false, hashCid("#4")),
      dtoDivulgence(Some(offset(5)), 3L, hashCid("#4")),
      dtoCompletion(offset(5)),
      // Transaction Metering
      dtoMetering("AppA", offset(1)),
      dtoMetering("AppB", offset(4)),
    )

    val readers = Set(signatory)

    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(someLedgerMeteringEnd))

    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))

    // Insert first batch of updates, but crash before writing the first ledger end
    executeSql(ingest(dtos1, _))

    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))

    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(10, 6L)))

    // Check the contents
    val parties2 = executeSql(backend.party.knownParties)
    val config2 = executeSql(backend.configuration.ledgerConfiguration)
    val packages2 = executeSql(backend.packageBackend.lfPackages)
    val contract42 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#4"),
      )
    )
    val contract92 = executeSql(
      backend.contract.activeContractWithoutArgument(
        readers,
        hashCid("#9"),
      )
    )
    val filterIds2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholders(
        stakeholder = someParty,
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    val metering2 =
      executeSql(backend.metering.read.reportData(Timestamp.Epoch, None, None))

    parties2 shouldBe empty
    packages2 shouldBe empty
    config2 shouldBe empty
    contract42 shouldBe None
    contract92 shouldBe None
    filterIds2 shouldBe empty
    metering2.applicationData shouldBe empty

  }
}
