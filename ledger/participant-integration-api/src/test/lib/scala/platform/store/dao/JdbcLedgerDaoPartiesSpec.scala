// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.appendonlydao.PersistenceResponse
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.platform.store.entries.PartyLedgerEntry.AllocationAccepted
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

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
      parties should contain.allOf(alice, bob)
    }
  }

  it should "store and retrieve accepted and rejected parties" in {
    val acceptedParty = Ref.Party.assertFromString(s"Accepted-${UUID.randomUUID()}")
    val nonExistentParty = UUID.randomUUID().toString
    val rejectionReason = s"$nonExistentParty is rejected"
    val accepted = PartyDetails(
      party = acceptedParty,
      displayName = Some("Accepted Ackbar"),
      isLocal = true,
    )
    val acceptedSubmissionId = UUID.randomUUID().toString
    val acceptedRecordTime = Timestamp.now()
    val accepted1 =
      PartyLedgerEntry.AllocationAccepted(Some(acceptedSubmissionId), acceptedRecordTime, accepted)
    val rejectedSubmissionId = UUID.randomUUID().toString
    val rejectedRecordTime = Timestamp.now()
    val rejected1 =
      PartyLedgerEntry.AllocationRejected(rejectedSubmissionId, rejectedRecordTime, rejectionReason)
    val originalOffset = previousOffset.get().get
    val offset1 = nextOffset()
    for {
      response1 <- storePartyEntry(
        accepted,
        offset1,
        Some(acceptedSubmissionId),
        acceptedRecordTime,
      )
      _ = response1 should be(PersistenceResponse.Ok)
      offset2 = nextOffset()
      response2 <- storeRejectedPartyEntry(
        rejectionReason,
        offset2,
        rejectedSubmissionId,
        rejectedRecordTime,
      )
      _ = response2 should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(acceptedParty, nonExistentParty))
      partyEntries <- ledgerDao
        .getPartyEntries(originalOffset, nextOffset())
        .take(4)
        .runWith(Sink.seq)
    } yield {
      parties should contain.only(accepted)
      assert(partyEntries == Vector((offset1, accepted1), (offset2, rejected1)))
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
      parties should contain.only(dan, eve)
    }
  }

  it should "be able to store multiple parties with the same identifier, which was local once, and the last update will be visible as query-ing, except is_local: that stays true" in {
    val danParty = Ref.Party.assertFromString(s"Dan-${UUID.randomUUID()}")
    val dan = PartyDetails(
      party = danParty,
      displayName = Some("Dangerous Dan"),
      isLocal = true,
    )
    val dan2 = dan.copy(displayName = Some("Even more dangerous Dan"))
    val dan3 = dan.copy(displayName = Some("Even more so dangerous Dan"))
    val dan4 = dan.copy(
      displayName = Some("Ultimately dangerous Dan"),
      isLocal = false,
    )
    val beforeStartOffset = nextOffset()
    val firstOffset = nextOffset()
    for {
      response <- storePartyEntry(dan, firstOffset)
      _ = response should be(PersistenceResponse.Ok)
      secondOffset = nextOffset()
      response <- storePartyEntry(dan2, secondOffset)
      _ = response should be(PersistenceResponse.Ok)
      thirdOffset = nextOffset()
      response <- storePartyEntry(dan3, thirdOffset)
      _ = response should be(PersistenceResponse.Ok)
      lastOffset = nextOffset()
      response <- storePartyEntry(
        dan4,
        lastOffset,
        Some(Ref.SubmissionId.assertFromString("final submission")),
      )
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(danParty))
      partyEntries <- ledgerDao
        .getPartyEntries(beforeStartOffset, nextOffset())
        .runWith(Sink.collection)
    } yield {
      parties shouldBe List(dan4.copy(isLocal = true)) // once local stays local
      val partyEntriesMap = partyEntries.toMap
      partyEntriesMap(firstOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan
      partyEntriesMap(secondOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan2
      partyEntriesMap(thirdOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan3
      partyEntriesMap(lastOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan4
      partyEntriesMap(lastOffset).submissionIdOpt shouldBe Some(
        Ref.SubmissionId.assertFromString("final submission")
      )
    }
  }

  it should "be able to store multiple parties with the same identifier, which was never local once, and the last update will be visible as query-ing, also is_local: false" in {
    val danParty = Ref.Party.assertFromString(s"Dan-${UUID.randomUUID()}")
    val dan = PartyDetails(
      party = danParty,
      displayName = Some("Dangerous Dan"),
      isLocal = false,
    )
    val dan2 = dan.copy(displayName = Some("Even more dangerous Dan"))
    val dan3 = dan.copy(displayName = Some("Even more so dangerous Dan"))
    val dan4 = dan.copy(displayName = Some("Ultimately dangerous Dan"))
    val beforeStartOffset = nextOffset()
    val firstOffset = nextOffset()
    for {
      response <- storePartyEntry(dan, firstOffset)
      _ = response should be(PersistenceResponse.Ok)
      secondOffset = nextOffset()
      response <- storePartyEntry(dan2, secondOffset)
      _ = response should be(PersistenceResponse.Ok)
      thirdOffset = nextOffset()
      response <- storePartyEntry(dan3, thirdOffset)
      _ = response should be(PersistenceResponse.Ok)
      lastOffset = nextOffset()
      response <- storePartyEntry(
        dan4,
        lastOffset,
        Some(Ref.SubmissionId.assertFromString("final submission")),
      )
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.getParties(Seq(danParty))
      partyEntries <- ledgerDao
        .getPartyEntries(beforeStartOffset, nextOffset())
        .runWith(Sink.collection)
    } yield {
      parties shouldBe List(dan4)
      val partyEntriesMap = partyEntries.toMap
      partyEntriesMap(firstOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan
      partyEntriesMap(secondOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan2
      partyEntriesMap(thirdOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan3
      partyEntriesMap(lastOffset).asInstanceOf[AllocationAccepted].partyDetails shouldBe dan4
      partyEntriesMap(lastOffset).submissionIdOpt shouldBe Some(
        Ref.SubmissionId.assertFromString("final submission")
      )
    }
  }

  private def storePartyEntry(
      partyDetails: PartyDetails,
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

  private def storeRejectedPartyEntry(
      reason: String,
      offset: Offset,
      submissionIdOpt: Ref.SubmissionId,
      recordTime: Timestamp,
  ): Future[PersistenceResponse] =
    ledgerDao
      .storePartyEntry(
        offset,
        PartyLedgerEntry.AllocationRejected(submissionIdOpt, recordTime, reason),
      )
      .map { response =>
        previousOffset.set(Some(offset))
        response
      }
}
