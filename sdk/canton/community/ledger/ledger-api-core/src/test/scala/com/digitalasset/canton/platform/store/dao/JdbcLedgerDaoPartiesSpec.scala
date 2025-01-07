// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

private[dao] trait JdbcLedgerDaoPartiesSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite with OptionValues =>

  behavior of "JdbcLedgerDao (parties)"

  it should "store and retrieve all parties" in {
    val alice = IndexerPartyDetails(
      party = Ref.Party.assertFromString(s"Alice-${UUID.randomUUID()}"),
      isLocal = true,
    )
    val bob = IndexerPartyDetails(
      party = Ref.Party.assertFromString(s"Bob-${UUID.randomUUID()}"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(alice, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(bob, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.listKnownParties(None, 1000)
    } yield {
      parties should contain.allOf(alice, bob)
    }
  }

  it should "retrieve all parties in two chunks" in {
    val randomSuffix = UUID.randomUUID()
    def genParty(name: String) =
      IndexerPartyDetails(
        party = Ref.Party.assertFromString(s"$name-$randomSuffix"),
        isLocal = true,
      )
    val newParties = List("Wes", "Zeb", "Les", "Mel").map(genParty)

    for {
      partiesBefore <- ledgerDao.listKnownParties(None, 1000)
      _ <- MonadUtil.sequentialTraverse_(newParties)(storePartyEntry(_, nextOffset()))
      parties1 <- ledgerDao.listKnownParties(None, partiesBefore.size + 1)
      parties2 <- ledgerDao.listKnownParties(
        parties1.lastOption.map(_.party),
        partiesBefore.size + newParties.size,
      )
    } yield {
      parties1 ++ parties2 should contain.allElementsOf(newParties)
      parties1 ++ parties2 should contain.allElementsOf(partiesBefore)
      parties1.size + parties2.size should equal(newParties.size + partiesBefore.size)
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
    val carol = IndexerPartyDetails(
      party = party,
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
    val dan = IndexerPartyDetails(
      party = danParty,
      isLocal = true,
    )
    val eve = IndexerPartyDetails(
      party = eveParty,
      isLocal = true,
    )
    for {
      response <- storePartyEntry(dan, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(eve, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(danParty, eveParty, nonExistentParty))
    } yield {
      parties should contain.only(dan, eve)
    }
  }

  private def storePartyEntry(
      partyDetails: IndexerPartyDetails,
      offset: Offset,
      submissionIdOpt: Option[Ref.SubmissionId] = Some(UUID.randomUUID().toString),
      recordTime: Timestamp = Timestamp.now(),
  ) =
    ledgerDao
      .storePartyEntry(
        offset,
        PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails),
      )
      .map { response =>
        previousOffset.set(Some(offset))
        response
      }
}
