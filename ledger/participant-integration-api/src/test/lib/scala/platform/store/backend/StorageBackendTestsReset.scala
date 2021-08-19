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
    val dtos: Vector[DbDto] = (
      dtoConfiguration(offset(1)) ++
        dtoParty(offset(2)) ++
        dtoPackage(offset(3)) ++
        dtoTransaction(offset(4), 1)
    ).toVector

    for {
      // Initialize and insert some data
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ledgerEnd(4, 4)))

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
