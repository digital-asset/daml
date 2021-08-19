// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReset extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (reset)"

  import StorageBackendTestValues._

  it should "start with an empty index" in {
    for {
      identity <- executeSql(backend.ledgerIdentity)
      end <- executeSql(backend.ledgerEnd)
      parties <- executeSql(backend.knownParties)
      config <- executeSql(backend.ledgerConfiguration)
      packages <- executeSql(backend.lfPackages)
      events <- executeSql(backend.contractStateEvents(0, Long.MaxValue))
    } yield {
      identity shouldBe None
      end shouldBe None
      parties shouldBe empty
      packages shouldBe empty
      events shouldBe empty
      config shouldBe None
    }
  }

  it should "reset contents of the index" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1)),
      // 2: party allocation
      dtoParty(offset(2)),
      dtoPartyEntry(offset(2)),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
      // 4: transaction with create node
      dtoCreate(offset(4), 1L, "#4"),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, true, "#4"),
      dtoDivulgence(offset(5), 3L, "#4"),
      dtoCompletion(offset(5)),
    )

    for {
      // Initialize and insert some data
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ledgerEnd(5, 3)))

      // Reset
      _ <- executeSql(backend.reset)

      // Check the contents
      identity <- executeSql(backend.ledgerIdentity)
      end <- executeSql(backend.ledgerEnd)
      parties <- executeSql(backend.knownParties)
      config <- executeSql(backend.ledgerConfiguration)
      packages <- executeSql(backend.lfPackages)
      events <- executeSql(backend.contractStateEvents(0, Long.MaxValue))
    } yield {
      identity shouldBe None
      end shouldBe None
      parties shouldBe empty
      packages shouldBe empty
      events shouldBe empty
      config shouldBe None
    }
  }
}
