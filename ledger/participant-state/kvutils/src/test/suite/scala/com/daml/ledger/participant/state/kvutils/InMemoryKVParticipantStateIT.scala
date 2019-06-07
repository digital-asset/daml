// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.Update.{PartyAddedToParticipant, PublicPackagesUploaded}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.AsyncWordSpec

class InMemoryKVParticipantStateIT extends AsyncWordSpec with AkkaBeforeAndAfterAll {

  private val emptyTransaction: SubmittedTransaction =
    PartialTransaction.initial.finish.right.get

  def submitterInfo(rt: Timestamp) = SubmitterInfo(
    submitter = Ref.Party.assertFromString("Alice"),
    applicationId = Ref.LedgerString.assertFromString("tests"),
    commandId = Ref.LedgerString.assertFromString("X"),
    maxRecordTime = rt.addMicros(100000)
  )
  def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests"))
  )



  "In-memory implementation" should {

    // FIXME(JM): Setup fixture for the participant-state
    // creation & teardown!

    "return initial conditions" in {
      val ps = new InMemoryKVParticipantState
      ps.getLedgerInitialConditions()
        .runWith(Sink.head)
        .map { _ =>
          ps.close()
          succeed
        }
    }

    "provide update after uploadPackages" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime()
      val submissionId = new AtomicInteger()

      val sourceDescription = "provided by test"
      val archive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build
      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset: Offset, update: PublicPackagesUploaded) =>
            ps.close()
            assert(offset == Offset(Array(0L)))
            assert(update.archives == List(archive))
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }

      ps.uploadPackages(submissionId.getAndIncrement().toString, List(archive), sourceDescription)
      waitForUpdateFuture
    }

    "provide update after allocateParty" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime()
      val submissionId = new AtomicInteger()

      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")
      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            ps.close()
            assert(offset == Offset(Array(0L)))
            assert(update.party == hint.get)
            assert(update.displayName == displayName.get)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }

      ps.allocateParty(submissionId.getAndIncrement().toString, hint, displayName)
      waitForUpdateFuture
    }

    "provide update after transaction submission" in {

      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close()
            assert(offset == Offset(Array(0L)))
        }

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      waitForUpdateFuture
    }

    "reject duplicate commands" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq).map { updates =>
          ps.close()

          val (offset1, update1) = updates.head
          val (offset2, update2) = updates(1)
          assert(offset1 == Offset(Array(0L)))
          assert(update1.isInstanceOf[Update.TransactionAccepted])

          assert(offset2 == Offset(Array(1L)))
          assert(update2.isInstanceOf[Update.CommandRejected])
          assert(
            update2
              .asInstanceOf[Update.CommandRejected]
              .reason == RejectionReason.DuplicateCommand)
        }

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      waitForUpdateFuture
    }

    "return second update with beginAfter=1" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = Some(Offset(Array(0L)))).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close()
            assert(offset == Offset(Array(1L)))
            assert(update.isInstanceOf[Update.CommandRejected])
        }
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      waitForUpdateFuture
    }
  }

}
