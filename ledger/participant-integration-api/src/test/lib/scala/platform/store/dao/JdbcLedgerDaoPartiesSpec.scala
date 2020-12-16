// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.indexer.{IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.dao.ParametersTable.LedgerEndUpdateError
import com.daml.platform.store.entries.PartyLedgerEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoPartiesSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (parties)"

  it should "store and retrieve all parties" in {
    val alice = PartyDetails(
      party = Ref.Party.assertFromString(s"Alice-${UUID.randomUUID()}"),
      displayName = Some("Alice Arkwright"),
      isLocal = true,
    )
    val bob = PartyDetails(
      party = Ref.Party.assertFromString(s"Bob-${UUID.randomUUID()}"),
      displayName = Some("Bob Bobertson"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(alice, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(bob, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.listKnownParties()
    } yield {
      parties should contain allOf (alice, bob)
    }
  }

  it should "retrieve zero parties" in {
    for {
      noPartyDetails <- ledgerDao.getParties(Seq.empty)
    } yield {
      noPartyDetails should be(Seq.empty)
    }
  }

  it should "retrieve a single party, if they exist" in {
    val party = Ref.Party.assertFromString(s"Carol-${UUID.randomUUID()}")
    val nonExistentParty = UUID.randomUUID().toString
    val carol = PartyDetails(
      party = party,
      displayName = Some("Carol Carlisle"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(carol, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      carolPartyDetails <- ledgerDao.getParties(Seq(party))
      noPartyDetails <- ledgerDao.getParties(Seq(nonExistentParty))
    } yield {
      carolPartyDetails should be(Seq(carol))
      noPartyDetails should be(Seq.empty)
    }
  }

  it should "retrieve multiple parties" in {
    val danParty = Ref.Party.assertFromString(s"Dan-${UUID.randomUUID()}")
    val eveParty = Ref.Party.assertFromString(s"Eve-${UUID.randomUUID()}")
    val nonExistentParty = UUID.randomUUID().toString
    val dan = PartyDetails(
      party = danParty,
      displayName = Some("Dangerous Dan"),
      isLocal = true,
    )
    val eve = PartyDetails(
      party = eveParty,
      displayName = Some("Dangerous Dan"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(dan, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(eve, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(danParty, eveParty, nonExistentParty))
    } yield {
      parties should contain only (dan, eve)
    }
  }

  it should "inform the caller if they try to write a duplicate party" in {
    val fred = PartyDetails(
      party = Ref.Party.assertFromString(s"Fred-${UUID.randomUUID()}"),
      displayName = Some("Fred Flintstone"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(fred, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(fred, nextOffset(), shouldUpdateLegerEnd = false)
    } yield {
      response should be(PersistenceResponse.Duplicate)
    }
  }

  it should "fail on storing a party entry with non-incremental offsets" in {
    val fred = PartyDetails(
      party = Ref.Party.assertFromString(s"Fred-${UUID.randomUUID()}"),
      displayName = Some("Fred Flintstone"),
      isLocal = true,
    )
    recoverToSucceededIf[LedgerEndUpdateError](
      ledgerDao.storePartyEntry(
        IncrementalOffsetStep(nextOffset(), nextOffset()),
        allocationAccepted(fred)
      ))
  }

  private def storePartyEntry(
      partyDetails: PartyDetails,
      offset: Offset,
      shouldUpdateLegerEnd: Boolean = true) =
    ledgerDao
      .storePartyEntry(
        OffsetStep(previousOffset.get(), offset),
        allocationAccepted(partyDetails)
      )
      .map { response =>
        if (shouldUpdateLegerEnd) previousOffset.set(Some(offset))
        response
      }

  private def allocationAccepted(partyDetails: PartyDetails): PartyLedgerEntry.AllocationAccepted =
    PartyLedgerEntry.AllocationAccepted(
      submissionIdOpt = Some(UUID.randomUUID().toString),
      recordTime = Instant.now,
      partyDetails = partyDetails,
    )
}
