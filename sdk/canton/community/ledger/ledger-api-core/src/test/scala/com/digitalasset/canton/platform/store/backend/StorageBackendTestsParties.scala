// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.{HasExecutionContext, LfPartyId}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsParties
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend (parties)"

  import StorageBackendTestValues.*

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPartyEntry(someOffset)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    val partiesBeforeLedgerEndUpdate = executeSql(backend.party.knownParties(None, 10))
    executeSql(
      updateLedgerEnd(someOffset, ledgerEndSequentialId = 0)
    )
    val partiesAfterLedgerEndUpdate = executeSql(backend.party.knownParties(None, 10))

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested party allocation.
    partiesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the party.
    partiesAfterLedgerEndUpdate should not be empty
  }

  it should "accumulate multiple party records into one response" in {
    val dtos = Vector(
      // singular non-local
      dtoPartyEntry(offset(1), "aaf", isLocal = false),
      // singular local
      dtoPartyEntry(offset(2), "bbt", isLocal = true),
      // desired values in last record
      dtoPartyEntry(offset(3), "cct", isLocal = false),
      dtoPartyEntry(offset(4), "cct", isLocal = false),
      dtoPartyEntry(offset(5), "cct", isLocal = true),
      // desired values in last record except of is-local
      dtoPartyEntry(offset(6), "ddt", isLocal = false),
      dtoPartyEntry(offset(7), "ddt", isLocal = true),
      dtoPartyEntry(offset(8), "ddt", isLocal = false),
      // desired values in last record, reject coming in the middle
      dtoPartyEntry(offset(9), "eef", isLocal = false),
      dtoPartyEntry(offset(10), "eef", isLocal = true, reject = true),
      dtoPartyEntry(offset(11), "eef", isLocal = false),
      // desired values in middle record, reject coming last
      dtoPartyEntry(offset(12), "fff", isLocal = false),
      dtoPartyEntry(offset(13), "fff", isLocal = false),
      dtoPartyEntry(offset(14), "fff", isLocal = true, reject = true),
      // desired values before ledger end, undesired accept after ledger end
      dtoPartyEntry(offset(15), "ggf", isLocal = false),
      dtoPartyEntry(offset(17), "ggf", isLocal = true),
      // desired values before ledger end, undesired reject after ledger end
      dtoPartyEntry(offset(16), "hhf", isLocal = false),
      dtoPartyEntry(offset(18), "hhf", isLocal = true, reject = true),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(
      updateLedgerEnd(offset(16), ledgerEndSequentialId = 0)
    )

    def validateEntries(entry: IndexerPartyDetails): Unit =
      entry.isLocal shouldBe entry.party.lastOption.contains('t')

    val allKnownParties = executeSql(backend.party.knownParties(None, 10))
    allKnownParties.length shouldBe 8
    allKnownParties.foreach(validateEntries)

    val pageOne = executeSql(backend.party.knownParties(None, 4))
    pageOne.length shouldBe 4
    pageOne.foreach(validateEntries)
    pageOne.exists(_.party == "aaf") shouldBe true
    pageOne.exists(_.party == "bbt") shouldBe true
    pageOne.exists(_.party == "cct") shouldBe true
    pageOne.exists(_.party == "ddt") shouldBe true

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("ddt")), 10))
    pageTwo.length shouldBe 4
    pageTwo.foreach(validateEntries)
    pageTwo.exists(_.party == "eef") shouldBe true
    pageTwo.exists(_.party == "fff") shouldBe true
    pageTwo.exists(_.party == "ggf") shouldBe true
    pageTwo.exists(_.party == "hhf") shouldBe true
  }

  it should "get all parties ordered by id using binary collation" in {
    val dtos = Vector(
      dtoPartyEntry(offset(1), "a", isLocal = false),
      dtoPartyEntry(offset(2), "a-", isLocal = false),
      dtoPartyEntry(offset(3), "b", isLocal = false),
      dtoPartyEntry(offset(4), "a_", isLocal = false),
      dtoPartyEntry(offset(5), "-a", isLocal = false),
      dtoPartyEntry(offset(6), "_a", isLocal = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(
      updateLedgerEnd(offset(6), ledgerEndSequentialId = 0)
    )

    val allKnownParties = executeSql(backend.party.knownParties(None, 10))
    allKnownParties.length shouldBe 6

    allKnownParties
      .map(_.party) shouldBe Seq("-a", "_a", "a", "a-", "a_", "b")

    val pageOne = executeSql(backend.party.knownParties(None, 3))
    pageOne.length shouldBe 3
    pageOne
      .map(_.party) shouldBe Seq("-a", "_a", "a")

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("a")), 10))
    pageTwo.length shouldBe 3
    pageTwo
      .map(_.party) shouldBe Seq("a-", "a_", "b")
  }
}
