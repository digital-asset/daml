// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (ingestion)"

  import StorageBackendTestValues._

  it should "ingest a single configuration update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoConfiguration(someOffset, someConfiguration)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val configBeforeLedgerEndUpdate = executeSql(backend.configuration.ledgerConfiguration)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val configAfterLedgerEndUpdate = executeSql(backend.configuration.ledgerConfiguration)

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

  it should "ingest a single package update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPackage(someOffset),
      dtoPackageEntry(someOffset),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val packagesBeforeLedgerEndUpdate = executeSql(backend.packageBackend.lfPackages)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val packagesAfterLedgerEndUpdate = executeSql(backend.packageBackend.lfPackages)

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested package upload.
    packagesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the package.
    packagesAfterLedgerEndUpdate should not be empty
  }

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPartyEntry(someOffset)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val partiesBeforeLedgerEndUpdate = executeSql(backend.party.knownParties)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val partiesAfterLedgerEndUpdate = executeSql(backend.party.knownParties)

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested party allocation.
    partiesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the party.
    partiesAfterLedgerEndUpdate should not be empty
  }

}
