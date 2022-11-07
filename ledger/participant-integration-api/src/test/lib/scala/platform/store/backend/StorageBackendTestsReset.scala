// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReset extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (reset)"

  import StorageBackendTestValues._

  it should "start with an empty index" in {
    val identity = executeSql(backend.parameter.ledgerIdentity)
    val end = executeSql(backend.parameter.ledgerEnd)
    val parties = executeSql(backend.party.knownParties)
    val config = executeSql(backend.configuration.ledgerConfiguration)
    val packages = executeSql(backend.packageBackend.lfPackages)
    val events = executeSql(backend.contract.contractStateEvents(0, Long.MaxValue))
    val stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    parties shouldBe empty
    packages shouldBe empty
    events shouldBe empty
    config shouldBe None
    stringInterningEntries shouldBe empty
  }

  it should "not see any data after advancing the ledger end" in {
    advanceLedgerEndToMakeOldDataVisible()
    val parties = executeSql(backend.party.knownParties)
    val config = executeSql(backend.configuration.ledgerConfiguration)
    val packages = executeSql(backend.packageBackend.lfPackages)

    parties shouldBe empty
    packages shouldBe empty
    config shouldBe None
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
      dtoCreate(offset(4), 1L, hashCid("#4")),
      DbDto.CreateFilter_Stakeholder(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, true, hashCid("#4")),
      dtoDivulgence(Some(offset(5)), 3L, hashCid("#4")),
      dtoCompletion(offset(5)),
    )

    // Initialize and insert some data
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(ledgerEnd(5, 3L)))

    // Reset
    executeSql(backend.reset.resetAll)

    // Check the contents (queries that do not depend on ledger end)
    val identity = executeSql(backend.parameter.ledgerIdentity)
    val end = executeSql(backend.parameter.ledgerEnd)
    val events = executeSql(backend.contract.contractStateEvents(0, Long.MaxValue))

    // Check the contents (queries that don't read beyond ledger end)
    advanceLedgerEndToMakeOldDataVisible()
    val parties = executeSql(backend.party.knownParties)
    val config = executeSql(backend.configuration.ledgerConfiguration)
    val packages = executeSql(backend.packageBackend.lfPackages)
    val stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )
    val filterIds = executeSql(
      backend.event.fetchIds_create_stakeholders(
        partyFilter = someParty,
        templateIdFilter = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    parties shouldBe empty
    packages shouldBe empty // Note: resetAll() does delete packages
    events shouldBe empty
    config shouldBe None
    stringInterningEntries shouldBe empty
    filterIds shouldBe empty
  }

  // Some queries are protected to never return data beyond the current ledger end.
  // By advancing the ledger end to a large value, we can check whether these
  // queries now find any left-over data not cleaned by reset.
  private def advanceLedgerEndToMakeOldDataVisible(): Unit = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(updateLedgerEnd(ledgerEnd(10000, 10000)))
    ()
  }
}
