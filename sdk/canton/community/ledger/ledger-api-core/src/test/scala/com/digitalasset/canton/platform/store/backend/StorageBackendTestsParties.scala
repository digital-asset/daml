// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexerPartyDetails
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

  it should "empty display name represent lack of display name" in {
    val dtos = Vector(
      dtoPartyEntry(offset(1), party = "party1", displayNameOverride = Some(Some(""))),
      dtoPartyEntry(
        offset(2),
        party = "party2",
        displayNameOverride = Some(Some("nonEmptyDisplayName")),
      ),
      dtoPartyEntry(offset(3), party = "party3", displayNameOverride = Some(None)),
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), ledgerEndSequentialId = 0)
    )

    {
      val knownParties = executeSql(backend.party.knownParties(None, 10))
      val party1 = knownParties.find(_.party == "party1").value
      val party2 = knownParties.find(_.party == "party2").value
      val party3 = knownParties.find(_.party == "party3").value
      party1.displayName shouldBe None
      party2.displayName shouldBe Some("nonEmptyDisplayName")
      party3.displayName shouldBe None
    }
    {
      val party1 = executeSql(
        backend.party.parties(parties = Seq(Ref.Party.assertFromString("party1")))
      ).headOption.value
      val party2 = executeSql(
        backend.party.parties(parties = Seq(Ref.Party.assertFromString("party2")))
      ).headOption.value
      val party3 = executeSql(
        backend.party.parties(parties = Seq(Ref.Party.assertFromString("party3")))
      ).headOption.value
      party1.displayName shouldBe None
      party2.displayName shouldBe Some("nonEmptyDisplayName")
      party3.displayName shouldBe None
    }
  }

  it should "accumulate multiple party records into one response" in {
    val dtos = Vector(
      // singular non-local
      dtoPartyEntry(offset(1), "aaf", isLocal = false, Some(Some("aaf"))),
      // singular local
      dtoPartyEntry(offset(2), "bbt", isLocal = true, Some(Some("bbt"))),
      // desired values in last record
      dtoPartyEntry(offset(3), "cct", isLocal = false, Some(Some("cc-"))),
      dtoPartyEntry(offset(4), "cct", isLocal = false, Some(Some("---"))),
      dtoPartyEntry(offset(5), "cct", isLocal = true, Some(Some("cct"))),
      // desired values in last record except of is-local
      dtoPartyEntry(offset(6), "ddt", isLocal = false, Some(Some("dd-"))),
      dtoPartyEntry(offset(7), "ddt", isLocal = true, Some(Some("---"))),
      dtoPartyEntry(offset(8), "ddt", isLocal = false, Some(Some("ddt"))),
      // desired values in last record, reject coming in the middle
      dtoPartyEntry(offset(9), "eef", isLocal = false, Some(Some("ee-"))),
      dtoPartyEntry(offset(10), "eef", isLocal = true, Some(Some("---")), reject = true),
      dtoPartyEntry(offset(11), "eef", isLocal = false, Some(Some("eef"))),
      // desired values in middle record, reject coming last
      dtoPartyEntry(offset(12), "fff", isLocal = false, Some(Some("ff-"))),
      dtoPartyEntry(offset(13), "fff", isLocal = false, Some(Some("fff"))),
      dtoPartyEntry(offset(14), "fff", isLocal = true, Some(Some("---")), reject = true),
      // desired values before ledger end, undesired accept after ledger end
      dtoPartyEntry(offset(15), "ggf", isLocal = false, Some(Some("ggf"))),
      dtoPartyEntry(offset(17), "ggf", isLocal = true, Some(Some("---"))),
      // desired values before ledger end, undesired reject after ledger end
      dtoPartyEntry(offset(16), "hhf", isLocal = false, Some(Some("hhf"))),
      dtoPartyEntry(offset(18), "hhf", isLocal = true, Some(Some("---")), reject = true),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(
      updateLedgerEnd(offset(16), ledgerEndSequentialId = 0)
    )

    def validateEntries(entry: IndexerPartyDetails): Unit = {
      entry.displayName shouldBe Some(entry.party)
      entry.isLocal shouldBe entry.party.lastOption.contains('t')
    }

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
}
