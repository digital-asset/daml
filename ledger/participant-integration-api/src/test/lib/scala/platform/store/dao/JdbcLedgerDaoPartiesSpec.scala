// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.participant.state.v1.{Offset, SubmissionId}
import com.daml.lf.data.Ref
import com.daml.platform.indexer.{IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.dao.ParametersTable.LedgerEndUpdateError
import com.daml.platform.store.entries.PartyLedgerEntry
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
    val acceptedRecordTime = Instant.now()
    val accepted1 =
      PartyLedgerEntry.AllocationAccepted(Some(acceptedSubmissionId), acceptedRecordTime, accepted)
    val rejectedSubmissionId = UUID.randomUUID().toString
    val rejectedRecordTime = Instant.now()
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

  it should "inform the caller if they try to write a duplicate party" in {
    val fred = PartyDetails(
      party = Ref.Party.assertFromString(s"Fred-${UUID.randomUUID()}"),
      displayName = Some("Fred Flintstone"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(fred, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(fred, nextOffset())
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
        PartyLedgerEntry.AllocationAccepted(Some(UUID.randomUUID().toString), Instant.now(), fred),
      )
    )
  }

  it should "store just offset when duplicate party update encountered" in {
    val party = Ref.Party.assertFromString(s"Alice-${UUID.randomUUID()}")
    val aliceDetails = PartyDetails(
      party,
      displayName = Some("Alice Arkwright"),
      isLocal = true,
    )
    val aliceAgain = PartyDetails(
      party,
      displayName = Some("Alice Carthwright"),
      isLocal = true,
    )
    val bobDetails = PartyDetails(
      party = Ref.Party.assertFromString(s"Bob-${UUID.randomUUID()}"),
      displayName = Some("Bob Bobertson"),
      isLocal = true,
    )
    for {
      response <- storePartyEntry(aliceDetails, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      response <- storePartyEntry(aliceAgain, nextOffset())
      _ = response should be(PersistenceResponse.Duplicate)
      response <- storePartyEntry(bobDetails, nextOffset())
      _ = response should be(PersistenceResponse.Ok)
      parties <- ledgerDao.listKnownParties()
    } yield {
      parties should contain.allOf(aliceDetails, bobDetails)
    }
  }

  private def storePartyEntry(
      partyDetails: PartyDetails,
      offset: Offset,
      submissionIdOpt: Option[SubmissionId] = Some(UUID.randomUUID().toString),
      recordTime: Instant = Instant.now(),
  ) =
    ledgerDao
      .storePartyEntry(
        OffsetStep(previousOffset.get(), offset),
        PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails),
      )
      .map { response =>
        previousOffset.set(Some(offset))
        response
      }

  private def storeRejectedPartyEntry(
      reason: String,
      offset: Offset,
      submissionIdOpt: SubmissionId,
      recordTime: Instant,
  ): Future[PersistenceResponse] =
    ledgerDao
      .storePartyEntry(
        OffsetStep(previousOffset.get(), offset),
        PartyLedgerEntry.AllocationRejected(submissionIdOpt, recordTime, reason),
      )
      .map { response =>
        previousOffset.set(Some(offset))
        response
      }
}
