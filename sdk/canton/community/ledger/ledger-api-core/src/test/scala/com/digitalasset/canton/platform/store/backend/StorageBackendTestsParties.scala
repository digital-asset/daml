// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.config.CantonRequireTypes.String185
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
  import com.digitalasset.daml.lf.data.Ref.Party.assertFromString as party

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPartyEntry(someOffset)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    val partiesBeforeLedgerEndUpdate = executeSql(backend.party.knownParties(None, None, 10))
    executeSql(
      updateLedgerEnd(someOffset, ledgerEndSequentialId = 0)
    )
    val partiesAfterLedgerEndUpdate = executeSql(backend.party.knownParties(None, None, 10))

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested party allocation.
    partiesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the party.
    partiesAfterLedgerEndUpdate should not be empty
  }

  it should "accumulate multiple party records into one response" in {
    val dtos = Vector(
      // singular non-local
      dtoPartyEntry(offset(1), party("aaf"), isLocal = false),
      // singular local
      dtoPartyEntry(offset(2), party("bbt"), isLocal = true),
      // desired values in last record
      dtoPartyEntry(offset(3), party("cct"), isLocal = false),
      dtoPartyEntry(offset(4), party("cct"), isLocal = false),
      dtoPartyEntry(offset(5), party("cct"), isLocal = true),
      // desired values in last record except of is-local
      dtoPartyEntry(offset(6), party("ddt"), isLocal = false),
      dtoPartyEntry(offset(7), party("ddt"), isLocal = true),
      dtoPartyEntry(offset(8), party("ddt"), isLocal = false),
      // desired values in last record, reject coming in the middle
      dtoPartyEntry(offset(9), party("eef"), isLocal = false),
      dtoPartyEntry(offset(10), party("eef"), isLocal = true, reject = true),
      dtoPartyEntry(offset(11), party("eef"), isLocal = false),
      // desired values in middle record, reject coming last
      dtoPartyEntry(offset(12), party("fff"), isLocal = false),
      dtoPartyEntry(offset(13), party("fff"), isLocal = false),
      dtoPartyEntry(offset(14), party("fff"), isLocal = true, reject = true),
      // desired values before ledger end, undesired accept after ledger end
      dtoPartyEntry(offset(15), party("ggf"), isLocal = false),
      dtoPartyEntry(offset(17), party("ggf"), isLocal = true),
      // desired values before ledger end, undesired reject after ledger end
      dtoPartyEntry(offset(16), party("hhf"), isLocal = false),
      dtoPartyEntry(offset(18), party("hhf"), isLocal = true, reject = true),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(
      updateLedgerEnd(offset(16), ledgerEndSequentialId = 0)
    )

    def validateEntries(entry: IndexerPartyDetails): Unit =
      entry.isLocal shouldBe entry.party.lastOption.contains('t')

    val allKnownParties = executeSql(backend.party.knownParties(None, None, 10))
    allKnownParties.length shouldBe 8
    allKnownParties.foreach(validateEntries)

    val pageOne = executeSql(backend.party.knownParties(None, None, 4))
    pageOne.length shouldBe 4
    pageOne.foreach(validateEntries)
    pageOne.exists(_.party == "aaf") shouldBe true
    pageOne.exists(_.party == "bbt") shouldBe true
    pageOne.exists(_.party == "cct") shouldBe true
    pageOne.exists(_.party == "ddt") shouldBe true

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("ddt")), None, 10))
    pageTwo.length shouldBe 4
    pageTwo.foreach(validateEntries)
    pageTwo.exists(_.party == "eef") shouldBe true
    pageTwo.exists(_.party == "fff") shouldBe true
    pageTwo.exists(_.party == "ggf") shouldBe true
    pageTwo.exists(_.party == "hhf") shouldBe true
  }

  it should "get all parties ordered by id using binary collation" in {
    val dtos = Vector(
      dtoPartyEntry(offset(1), party("a"), isLocal = false),
      dtoPartyEntry(offset(2), party("a-"), isLocal = false),
      dtoPartyEntry(offset(3), party("b"), isLocal = false),
      dtoPartyEntry(offset(4), party("a_"), isLocal = false),
      dtoPartyEntry(offset(5), party("-a"), isLocal = false),
      dtoPartyEntry(offset(6), party("_a"), isLocal = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(
      updateLedgerEnd(offset(6), ledgerEndSequentialId = 0)
    )

    val allKnownParties = executeSql(backend.party.knownParties(None, None, 10))
    allKnownParties.length shouldBe 6

    val filteredParties =
      executeSql(backend.party.knownParties(None, Some(String185.tryCreate("a-")), 10))
    filteredParties.length shouldBe 1

    allKnownParties
      .map(_.party) shouldBe Seq("-a", "_a", "a", "a-", "a_", "b")

    val pageOne = executeSql(backend.party.knownParties(None, None, 3))
    pageOne.length shouldBe 3
    pageOne
      .map(_.party) shouldBe Seq("-a", "_a", "a")

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("a")), None, 10))
    pageTwo.length shouldBe 3
    pageTwo
      .map(_.party) shouldBe Seq("a-", "a_", "b")
  }

}
