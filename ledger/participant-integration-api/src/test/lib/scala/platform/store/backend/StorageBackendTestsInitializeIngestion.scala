// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitializeIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (initializeIngestion)"

  import StorageBackendTestValues._

  it should "delete overspill entries" in {
    val dtos1: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1), someConfiguration),
      // 2: party allocation
      dtoPartyEntry(offset(2), "party1"),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
      // 4: transaction with create node
      dtoCreate(offset(4), 1L, "#4"),
      DbDto.CreateFilter(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, false, "#4"),
      dtoDivulgence(Some(offset(5)), 3L, "#4"),
      dtoCompletion(offset(5)),
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
      dtoCreate(offset(9), 4L, "#9"),
      DbDto.CreateFilter(4L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(9)),
      // 10: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(10), 5L, false, "#9"),
      dtoDivulgence(Some(offset(10)), 6L, "#9"),
      dtoCompletion(offset(10)),
    )

    // TODO: make sure it's obvious these are the stakeholders of dtoCreate() nodes created above
    val readers = Set(Ref.Party.assertFromString("signatory"))

    for {
      // Initialize
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))

      // Start the indexer (a no-op in this case)
      end1 <- executeSql(backend.parameter.ledgerEnd)
      _ <- executeSql(backend.ingestion.deletePartiallyIngestedData(end1))

      // Fully insert first batch of updates
      _ <- executeSql(ingest(dtos1, _))
      _ <- executeSql(updateLedgerEnd(ledgerEnd(5, 3L)))

      // Partially insert second batch of updates (indexer crashes before updating ledger end)
      _ <- executeSql(ingest(dtos2, _))

      // Check the contents
      parties1 <- executeSql(backend.party.knownParties)
      config1 <- executeSql(backend.configuration.ledgerConfiguration)
      packages1 <- executeSql(backend.packageBackend.lfPackages)
      contract41 <- executeSql(
        backend.contract.activeContractWithoutArgument(
          readers,
          ContractId.V0.assertFromString("#4"),
        )
      )
      contract91 <- executeSql(
        backend.contract.activeContractWithoutArgument(
          readers,
          ContractId.V0.assertFromString("#9"),
        )
      )
      filterIds1 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = someParty,
          templateIdFilter = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
      )

      // Restart the indexer - should delete data from the partial insert above
      end2 <- executeSql(backend.parameter.ledgerEnd)
      _ <- executeSql(backend.ingestion.deletePartiallyIngestedData(end2))

      // Move the ledger end so that any non-deleted data would become visible
      _ <- executeSql(updateLedgerEnd(ledgerEnd(10, 6L)))

      // Check the contents
      parties2 <- executeSql(backend.party.knownParties)
      config2 <- executeSql(backend.configuration.ledgerConfiguration)
      packages2 <- executeSql(backend.packageBackend.lfPackages)
      contract42 <- executeSql(
        backend.contract.activeContractWithoutArgument(
          readers,
          ContractId.V0.assertFromString("#4"),
        )
      )
      contract92 <- executeSql(
        backend.contract.activeContractWithoutArgument(
          readers,
          ContractId.V0.assertFromString("#9"),
        )
      )
      filterIds2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = someParty,
          templateIdFilter = None,
          startExclusive = 0,
          endInclusive = 1000,
          limit = 1000,
        )
      )
    } yield {
      parties1 should have length 1
      packages1 should have size 1
      config1 shouldBe Some(offset(1) -> someConfiguration)
      contract41 should not be empty
      contract91 shouldBe None
      filterIds1 shouldBe List(1L, 4L) // since ledger-end does not limit the range query

      parties2 should have length 1
      packages2 should have size 1
      config2 shouldBe Some(offset(1) -> someConfiguration)
      contract42 should not be empty
      contract92 shouldBe None
      filterIds2 shouldBe List(1L)
    }
  }
}
