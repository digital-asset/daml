// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

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

  it should "not see any data after advancing the ledger end" in {
    for {
      _ <- advanceLedgerEndToMakeOldDataVisible()
      parties <- executeSql(backend.knownParties)
      config <- executeSql(backend.ledgerConfiguration)
      packages <- executeSql(backend.lfPackages)
    } yield {
      parties shouldBe empty
      packages shouldBe empty
      config shouldBe None
    }
  }

  it should "reset everything except packages when using reset" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1)),
      // 2: party allocation
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
      _ <- executeSql(backend.updateLedgerEnd(ledgerEnd(5, 3L)))

      // Reset
      _ <- executeSql(backend.reset)

      // Check the contents
      identity <- executeSql(backend.ledgerIdentity)
      end <- executeSql(backend.ledgerEnd)
      events <- executeSql(backend.contractStateEvents(0, Long.MaxValue))

      // Check the contents (queries that don't read beyond ledger end)
      _ <- advanceLedgerEndToMakeOldDataVisible()
      parties <- executeSql(backend.knownParties)
      config <- executeSql(backend.ledgerConfiguration)
      packages <- executeSql(backend.lfPackages)
    } yield {
      identity shouldBe None
      end shouldBe None
      parties shouldBe empty
      packages should not be empty // Note: reset() does not delete packages
      events shouldBe empty
      config shouldBe None
    }
  }

  it should "reset everything when using resetAll" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1)),
      // 2: party allocation
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
      _ <- executeSql(backend.updateLedgerEnd(ledgerEnd(5, 3L)))

      // Reset
      _ <- executeSql(backend.resetAll)

      // Check the contents (queries that do not depend on ledger end)
      identity <- executeSql(backend.ledgerIdentity)
      end <- executeSql(backend.ledgerEnd)
      events <- executeSql(backend.contractStateEvents(0, Long.MaxValue))

      // Check the contents (queries that don't read beyond ledger end)
      _ <- advanceLedgerEndToMakeOldDataVisible()
      parties <- executeSql(backend.knownParties)
      config <- executeSql(backend.ledgerConfiguration)
      packages <- executeSql(backend.lfPackages)
    } yield {
      identity shouldBe None
      end shouldBe None
      parties shouldBe empty
      packages shouldBe empty // Note: resetAll() does delete packages
      events shouldBe empty
      config shouldBe None
    }
  }

  // Some queries are protected to never return data beyond the current ledger end.
  // By advancing the ledger end to a large value, we can check whether these
  // queries now find any left-over data not cleaned by reset.
  private def advanceLedgerEndToMakeOldDataVisible(): Future[Unit] = {
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(backend.updateLedgerEnd(ledgerEnd(10000, 10000)))
    } yield ()
  }
}
