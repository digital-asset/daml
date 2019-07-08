// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.Update.{PartyAddedToParticipant, PublicPackageUploaded}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.google.protobuf.ByteString
import org.scalatest.AsyncWordSpec

import scala.compat.java8.FutureConverters._

class InMemoryKVParticipantStateIT extends AsyncWordSpec with AkkaBeforeAndAfterAll {

  private val emptyTransaction: SubmittedTransaction =
    PartialTransaction.initial.finish.right.get

  val participantId: ParticipantId = Ref.LedgerString.assertFromString("in-memory-participant")

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
      val ps = new InMemoryKVParticipantState(participantId)
      ps.getLedgerInitialConditions()
        .runWith(Sink.head)
        .map { _ =>
          ps.close()
          succeed
        }
    }

    "provide update after uploadPackages" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val sourceDescription = Some("provided by test")
      val archive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .setPayload(ByteString.copyFromUtf8("AAAAAAAHHHHHH"))
        .build

      for {
        result <- ps
          .uploadPackages(List(archive), sourceDescription)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        ps.close()
        result match {
          case UploadPackagesResult.Ok =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
        updateTuple match {
          case (offset: Offset, update: PublicPackageUploaded) =>
            ps.close()
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.archive == archive)
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }
      }
    }

    "provide two updates after uploadPackages with two archives" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val sourceDescription = Some("provided by test")
      val archives = List(
        DamlLf.Archive.newBuilder
          .setHash("asdf")
          .setPayload(ByteString.copyFromUtf8("AAAAAAAHHHHHH"))
          .build,
        DamlLf.Archive.newBuilder
          .setHash("zxcv")
          .setPayload(ByteString.copyFromUtf8("ZZZZZZZZZZZZZ"))
          .build
      )

      for {
        result <- ps
          .uploadPackages(archives, sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case UploadPackagesResult.Ok =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
        updateTuples.head match {
          case (offset: Offset, update: PublicPackageUploaded) =>
            ps.close()
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.archive == archives.head)
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }
        updateTuples(1) match {
          case (offset: Offset, update: PublicPackageUploaded) =>
            ps.close()
            assert(offset == Offset(Array(0L, 1L)))
            assert(update.archive == archives(1))
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }
      }
    }

    "remove duplicate package from update after uploadPackages" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val sourceDescription = Some("provided by test")
      val archive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .setPayload(ByteString.copyFromUtf8("AAAAAAAHHHHHH"))
        .build
      val otherArchive = DamlLf.Archive.newBuilder
        .setHash("zxcv")
        .setPayload(ByteString.copyFromUtf8("ZZZZZZZZZZZZZ"))
        .build

      for {
        _ <- ps
          .uploadPackages(List(archive), sourceDescription)
          .toScala
        result <- ps
          .uploadPackages(List(archive), sourceDescription)
          .toScala
        _ <- ps
          .uploadPackages(List(otherArchive), sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case UploadPackagesResult.Ok =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
        // first upload arrives as head update:
        updateTuples.head match {
          case (offset: Offset, update: PublicPackageUploaded) =>
            ps.close()
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.archive == archive)
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }
        // second upload results in no update because it was a duplicate
        // third upload arrives as a second update:
        updateTuples(1) match {
          case (offset: Offset, update: PublicPackageUploaded) =>
            ps.close()
            assert(offset == Offset(Array(2L, 0L)))
            assert(update.archive == otherArchive)
            assert(update.sourceDescription == sourceDescription)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a package upload")
        }
      }
    }

    "reject uploadPackages when archive is empty" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val sourceDescription = Some("provided by test")
      val archive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps
          .uploadPackages(List(archive), sourceDescription)
          .toScala
      } yield {
        ps.close()
        result match {
          case UploadPackagesResult.InvalidPackage(_) =>
            succeed
          case _ =>
            fail("unexpected response to package upload")
        }
      }
    }

    "provide update after allocateParty" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      for {
        allocResult <- ps
          .allocateParty(hint, displayName)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        ps.close()
        allocResult match {
          case PartyAllocationResult.Ok(partyDetails) =>
            assert(partyDetails.party == hint.get)
            assert(partyDetails.displayName == displayName)
            assert(partyDetails.isLocal)
          case _ =>
            fail("unexpected response to party allocation")
        }
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party == hint.get)
            assert(update.displayName == displayName.get)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ => fail("unexpected update message after a party allocation")
        }
      }
    }

    "accept allocateParty when hint is empty" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val hint = None
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint, displayName).toScala
      } yield {
        ps.close()
        result match {
          case PartyAllocationResult.Ok(_) =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
      }
//      ps.allocateParty(hint, displayName).thenApply[Assertion]({
//        case PartyAllocationResult.InvalidName(_) =>
//          ps.close()
//          succeed
//        case _ =>
//          ps.close()
//          fail("unexpected response to party allocation")
//      }).toScala
    }

    "reject allocateParty when hint contains invalid string for a party" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val hint = Some("Alice!@")
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint, displayName).toScala
      } yield {
        ps.close()
        result match {
          case PartyAllocationResult.InvalidName(_) =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
      }
    }

    "reject duplicate allocateParty" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      for {
        result1 <- ps.allocateParty(hint, displayName).toScala
        result2 <- ps.allocateParty(hint, displayName).toScala
      } yield {
        ps.close()
        result1 match {
          case PartyAllocationResult.Ok(_) =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }

        result2 match {
          case PartyAllocationResult.AlreadyExists =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
      }
    }

    "provide update after transaction submission" in {

      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close()
            assert(offset == Offset(Array(0L, 0L)))
        }

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      waitForUpdateFuture
    }

    "reject duplicate commands" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq).map { updates =>
          ps.close()

          val (offset1, update1) = updates.head
          val (offset2, update2) = updates(1)
          assert(offset1 == Offset(Array(0L, 0L)))
          assert(update1.isInstanceOf[Update.TransactionAccepted])

          assert(offset2 == Offset(Array(1L, 0L)))
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

    "return second update with beginAfter=0" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close()
            assert(offset == Offset(Array(1L, 0L)))
            assert(update.isInstanceOf[Update.CommandRejected])
        }
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      waitForUpdateFuture
    }
  }

  "return update [0,1] with beginAfter=[0,0]" in {
    val ps = new InMemoryKVParticipantState(participantId)
    val rt = ps.getNewRecordTime()

    val sourceDescription = Some("provided by test")
    val archives = List(
      DamlLf.Archive.newBuilder
        .setHash("asdf")
        .setPayload(ByteString.copyFromUtf8("AAAAAAAHHHHHH"))
        .build,
      DamlLf.Archive.newBuilder
        .setHash("zxcv")
        .setPayload(ByteString.copyFromUtf8("ZZZZZZZZZZZZZ"))
        .build
    )

    for {
      _ <- ps
        .uploadPackages(archives, sourceDescription)
        .toScala
      updateTuple <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head)
    } yield {
      ps.close()
      updateTuple match {
        case (offset, update: PublicPackageUploaded) =>
          ps.close()
          assert(offset == Offset(Array(0L, 1L)))
          assert(update.archive == archives(1))
          assert(update.sourceDescription == sourceDescription)
          assert(update.participantId == ps.participantId)
          assert(update.recordTime >= rt)
        case _ => fail("unexpected update message after a package upload")
      }
    }
  }
}
