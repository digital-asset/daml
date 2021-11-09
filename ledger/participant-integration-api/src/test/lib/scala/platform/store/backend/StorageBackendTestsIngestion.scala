// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (ingestion)"

  import StorageBackendTestValues._

  it should "ingest a single configuration update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoConfiguration(someOffset, someConfiguration)
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      configBeforeLedgerEndUpdate <- executeSql(backend.configuration.ledgerConfiguration)
      _ <- executeSql(
        updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      configAfterLedgerEndUpdate <- executeSql(backend.configuration.ledgerConfiguration)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested configuration change.
      configBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the configuration change.
      inside(configAfterLedgerEndUpdate) { case Some((offset, config)) =>
        offset shouldBe someOffset
        config shouldBe someConfiguration
      }
      configAfterLedgerEndUpdate should not be empty
    }
  }

  it should "ingest a single package update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPackage(someOffset),
      dtoPackageEntry(someOffset),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      packagesBeforeLedgerEndUpdate <- executeSql(backend.packageBackend.lfPackages)
      _ <- executeSql(
        updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      packagesAfterLedgerEndUpdate <- executeSql(backend.packageBackend.lfPackages)
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
    val dtos = Vector(
      dtoPartyEntry(someOffset)
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      partiesBeforeLedgerEndUpdate <- executeSql(backend.party.knownParties)
      _ <- executeSql(
        updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      partiesAfterLedgerEndUpdate <- executeSql(backend.party.knownParties)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested party allocation.
      partiesBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the party.
      partiesAfterLedgerEndUpdate should not be empty
    }
  }

}
