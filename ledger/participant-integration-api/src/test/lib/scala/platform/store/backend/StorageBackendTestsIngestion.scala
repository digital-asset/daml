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

  private val parameterStorageBackend: ParameterStorageBackend =
    backendFactory.createParameterStorageBackend
  private val configurationStorageBackend: ConfigurationStorageBackend =
    backendFactory.createConfigurationStorageBackend
  private val partyStorageBackend: PartyStorageBackend = backendFactory.createPartyStorageBackend
  private val packageStorageBackend: PackageStorageBackend =
    backendFactory.createPackageStorageBackend

  behavior of "StorageBackend (ingestion)"

  import StorageBackendTestValues._

  it should "ingest a single configuration update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoConfiguration(someOffset, someConfiguration)
    )

    for {
      _ <- executeSql(parameterStorageBackend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      configBeforeLedgerEndUpdate <- executeSql(configurationStorageBackend.ledgerConfiguration)
      _ <- executeSql(
        parameterStorageBackend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      configAfterLedgerEndUpdate <- executeSql(configurationStorageBackend.ledgerConfiguration)
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
      _ <- executeSql(parameterStorageBackend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      packagesBeforeLedgerEndUpdate <- executeSql(packageStorageBackend.lfPackages)
      _ <- executeSql(
        parameterStorageBackend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      packagesAfterLedgerEndUpdate <- executeSql(packageStorageBackend.lfPackages)
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
      _ <- executeSql(parameterStorageBackend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      partiesBeforeLedgerEndUpdate <- executeSql(partyStorageBackend.knownParties)
      _ <- executeSql(
        parameterStorageBackend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(someOffset, 0))
      )
      partiesAfterLedgerEndUpdate <- executeSql(partyStorageBackend.knownParties)
    } yield {
      // The first query is executed before the ledger end is updated.
      // It should not see the already ingested party allocation.
      partiesBeforeLedgerEndUpdate shouldBe empty

      // The second query should now see the party.
      partiesAfterLedgerEndUpdate should not be empty
    }
  }

}
