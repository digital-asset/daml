// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsIngestion extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (ingestion)"

  import StorageBackendTestValues._

  it should "ingest a single configuration update" in {
    val someOffset = offset(1)
    val dtos = dtoConfiguration(someOffset).toVector
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      configBeforeLedgerEndUpdate <- executeSql(backend.ledgerConfiguration)
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0)))
      configAfterLedgerEndUpdate <- executeSql(backend.ledgerConfiguration)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested configuration change.
      configBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the configuration change.
      configAfterLedgerEndUpdate should not be empty
    }
  }

  it should "ingest a single package update" in {
    val someOffset = offset(1)
    val dtos = dtoPackage(someOffset).toVector
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      packagesBeforeLedgerEndUpdate <- executeSql(backend.lfPackages)
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0)))
      packagesAfterLedgerEndUpdate <- executeSql(backend.lfPackages)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested package upload.
      packagesBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the package.
      packagesAfterLedgerEndUpdate should not be empty
    }
  }

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = dtoParty(someOffset).toVector
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      partiesBeforeLedgerEndUpdate <- executeSql(backend.knownParties)
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0)))
      partiesAfterLedgerEndUpdate <- executeSql(backend.knownParties)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested party allocation.
      partiesBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the party.
      partiesAfterLedgerEndUpdate should not be empty
    }
  }
}
