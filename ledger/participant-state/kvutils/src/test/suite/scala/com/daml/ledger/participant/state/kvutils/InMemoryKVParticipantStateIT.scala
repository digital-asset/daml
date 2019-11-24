// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.Duration

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.backport.TimeModel
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.{Assertion, AsyncWordSpec}

import scala.compat.java8.FutureConverters._
import scala.util.Try

class InMemoryKVParticipantStateIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with BazelRunfiles {

  private val emptyTransaction: SubmittedTransaction =
    PartialTransaction.initial.finish.right.get

  private val participantId: ParticipantId =
    Ref.LedgerString.assertFromString("in-memory-participant")
  private val sourceDescription = Some("provided by test")

  private val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }
  private val archives =
    darReader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar"))).get.all

  private def submitterInfo(rt: Timestamp, party: String = "Alice") = SubmitterInfo(
    submitter = Ref.Party.assertFromString(party),
    applicationId = Ref.LedgerString.assertFromString("tests"),
    commandId = Ref.LedgerString.assertFromString("X"),
    maxRecordTime = rt.addMicros(Duration.ofSeconds(10).toNanos / 1000)
  )

  private def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests"))
  )

  private def matchPackageUpload(
      updateTuple: (Offset, Update),
      givenOffset: Offset,
      givenArchive: DamlLf.Archive,
      rt: Timestamp): Assertion = updateTuple match {
    case (offset: Offset, update: PublicPackageUploaded) =>
      assert(offset == givenOffset)
      assert(update.archive == givenArchive)
      assert(update.sourceDescription == sourceDescription)
      assert(update.participantId == participantId)
      assert(update.recordTime >= rt)
    case _ => fail("unexpected update message after a package upload")
  }

  private def matchPackageUploadEntryAccepted(
      updateTuple: (Offset, Update),
      givenOffset: Offset): Assertion = updateTuple match {
    case (offset: Offset, update: PackageUploadEntryAccepted) =>
      assert(offset == givenOffset)
      assert(update.participantId == participantId)
    case _ => fail("did not find expected upload entry accepted")
  }

  private def matchPackageUploadEntryRejected(
      updateTuple: (Offset, Update),
      givenOffset: Offset): Assertion = updateTuple match {
    case (offset: Offset, update: PackageUploadEntryRejected) =>
      assert(offset == givenOffset)
      assert(update.participantId == participantId)
      assert(update.reason contains ("rejected as invalid"))
    case _ => fail("did not find expected upload entry rejected")
  }

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

      for {
        result <- ps
          .uploadPackages(List(archives.head), sourceDescription)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("Unexpected response to package upload.  Error : " + result.toString)
        }
        matchPackageUpload(updateTuple, Offset(Array(0L, 0L)), archives.head, rt)
      }
    }

    // TODO BH: Many of these tests for transformation from DamlLogEntry to Update better belong as
    // a KeyValueConsumptionSpec as the heart of the logic is there
    "provide three updates and an accepted after uploadPackages with three archives" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      for {
        result <- ps
          .uploadPackages(archives, sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(4).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("Unexpected response to package upload.  Error : " + result.toString)
        }
        matchPackageUpload(updateTuples.head, Offset(Array(0L, 0L)), archives.head, rt)
        matchPackageUpload(updateTuples(1), Offset(Array(0L, 1L)), archives(1), rt)
        matchPackageUpload(updateTuples(2), Offset(Array(0L, 2L)), archives(2), rt)
        matchPackageUploadEntryAccepted(updateTuples(3), Offset(Array(0L, 3L)))
      }
    }

    "not provide update for uploadPackages with a duplicate package" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      for {
        _ <- ps
          .uploadPackages(List(archives.head), sourceDescription)
          .toScala
        duplicateResult <- ps
          .uploadPackages(List(archives.head), sourceDescription)
          .toScala
        _ <- ps
          .uploadPackages(List(archives(1)), sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(5).runWith(Sink.seq)
      } yield {
        ps.close()
        duplicateResult match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("Unexpected response to package upload.  Error : " + duplicateResult.toString)
        }
        // first upload arrives as head update followed by entry accepted:
        matchPackageUpload(updateTuples.head, Offset(Array(0L, 0L)), archives.head, rt)
        matchPackageUploadEntryAccepted(updateTuples(1), Offset(Array(0L, 1L)))
        // second upload results in no package upload but there is an entry accepted as it was a duplicate
        matchPackageUploadEntryAccepted(updateTuples(2), Offset(Array(1L, 0L)))
        // third upload arrives as a second package upload followed by an entry accepted:
        matchPackageUpload(updateTuples(3), Offset(Array(2L, 0L)), archives(1), rt)
        matchPackageUploadEntryAccepted(updateTuples(4), Offset(Array(2L, 1L)))
      }
    }

    "provide entry rejected for uploadPackages with an empty archive" in {
      val ps = new InMemoryKVParticipantState(participantId)

      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps
          .uploadPackages(List(badArchive), sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("Unexpected response to package upload.  Error : " + result.toString)
        }
        matchPackageUploadEntryRejected(updateTuples.head, Offset(Array(0L, 0L)))
        // there should be no package upload -- ensure next log entry is a heartbeat
        assert(updateTuples(1)._2.isInstanceOf[Heartbeat])
      }
    }

    "provide entry rejected for uploadPackages with a mixture of valid and invalid packages" in {
      val ps = new InMemoryKVParticipantState(participantId)

      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps
          .uploadPackages(List(badArchive, archives.head, archives(1)), sourceDescription)
          .toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(1).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("Unexpected response to package upload.  Error : " + result.toString)
        }
        matchPackageUploadEntryRejected(updateTuples.head, Offset(Array(0L, 0L)))
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
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("unexpected response to party allocation.  Error : " + allocResult.description)
        }
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party == hint.get)
            assert(update.displayName == displayName.get)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + allocResult.description)
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
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("unexpected response to party allocation.  Error : " + result.description)
        }
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party != hint)
            assert(update.displayName == displayName.get)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result.description)
        }
      }
    }

    "reject allocateParty when hint contains invalid string for a party" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime()

      val hint = Some("Alice!@")
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint, displayName).toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(1).runWith(Sink.seq)
      } yield {
        ps.close()
        result match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("unexpected response to party allocation.  Error : " + result.description)
        }
        updateTuples.head match {
          case (offset: Offset, update: PartyAllocationEntryRejected) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.rejectionReason contains "Party name is invalid")
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result.description)
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
        updateTuples <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        ps.close()
        result1 match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("unexpected response to party allocation.  Error : " + result1.description)
        }
        result2 match {
          case SubmissionResult.Acknowledged =>
            succeed
          case _ =>
            fail("unexpected response to party allocation.  Error : " + result2.description)
        }
        updateTuples(1) match {
          case (offset: Offset, update: PartyAllocationEntryRejected) =>
            assert(offset == Offset(Array(1L, 0L)))
            assert(update.rejectionReason equalsIgnoreCase "Party already exists")
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result2.description)
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

    "correctly implement open world tx submission authorization" in {
      val ps = new InMemoryKVParticipantState(participantId, openWorld = true)
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq).map { updates =>
          ps.close()
          val (offset1, update1) = updates(0)
          assert(offset1 == Offset(Array(0L, 0L)))
          assert(update1.isInstanceOf[Update.TransactionAccepted])

          val (offset2, update2) = updates(1)
          assert(offset2 == Offset(Array(1L, 0L)))
          assert(update2.isInstanceOf[Update.PartyAddedToParticipant])

          val (offset3, update3) = updates(2)
          assert(offset3 == Offset(Array(2L, 0L)))
          assert(update3.isInstanceOf[Update.TransactionAccepted])
        }
      val subInfo = submitterInfo(rt)

      for {
        // Submit without allocation in open world setting, expecting this to succeed.
        _ <- ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction).toScala

        // Allocate a party and try the submission again with an allocated party.
        allocResult <- ps
          .allocateParty(
            None /* no name hint, implementation decides party name */,
            Some("Somebody"))
          .toScala
        _ <- assert(allocResult.isInstanceOf[SubmissionResult])
        newParty <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head).map(_._2.asInstanceOf[PartyAddedToParticipant].party)
        _ <- ps
          .submitTransaction(
            submitterInfo(
              rt,
              party = newParty),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        r <- waitForUpdateFuture
      } yield (r)
    }

    "correctly implements closed world tx submission authorization" in {
      val ps = new InMemoryKVParticipantState(participantId, openWorld = false)
      val rt = ps.getNewRecordTime()

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq).map { updates =>
          ps.close()

          def takeUpdate(n: Int) = {
            val (offset, update) = updates(n)
            assert(offset == Offset(Array(n.toLong, 0L)))
            update
          }

          assert(
            takeUpdate(0)
              .asInstanceOf[Update.CommandRejected]
              .reason == RejectionReason.PartyNotKnownOnLedger)

          assert(takeUpdate(1).isInstanceOf[Update.PartyAddedToParticipant])
          assert(takeUpdate(2).isInstanceOf[Update.TransactionAccepted])
        }

      for {
        // Submit without allocation in closed world setting.
        _ <- ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction).toScala

        // Allocate a party and try the submission again with an allocated party.
        allocResult <- ps
          .allocateParty(
            None /* no name hint, implementation decides party name */,
            Some("Somebody"))
          .toScala
        _ <- assert(allocResult.isInstanceOf[SubmissionResult])
        newParty <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head).map(_._2.asInstanceOf[PartyAddedToParticipant].party)
        _ <- ps
          .submitTransaction(
            submitterInfo(
              rt,
              party = newParty),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        r <- waitForUpdateFuture
      } yield (r)
    }
  }

  "can submit new configuration" in {
    val ps = new InMemoryKVParticipantState(participantId)
    val rt = ps.getNewRecordTime()

    for {
      lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

      // Submit a configuration change that flips the "open world" flag.
      _ <- ps
        .submitConfiguration(
          maxRecordTime = rt.addMicros(1000000),
          submissionId = "test1",
          config = lic.config.copy(
            generation = lic.config.generation + 1,
            openWorld = !lic.config.openWorld
          ))
        .toScala

      // Submit another configuration change that uses stale "current config".
      _ <- ps
        .submitConfiguration(
          maxRecordTime = rt.addMicros(1000000),
          submissionId = "test2",
          config = lic.config.copy(
            timeModel = TimeModel(
              Duration.ofSeconds(123),
              Duration.ofSeconds(123),
              Duration.ofSeconds(123)).get
          )
        )
        .toScala

      updates <- ps.stateUpdates(None).take(2).runWith(Sink.seq)
    } yield {
      ps.close()
      // The first submission should change the config.
      val newConfig = updates(0)._2.asInstanceOf[Update.ConfigurationChanged]
      assert(newConfig.newConfiguration != lic.config)

      // The second submission should get rejected.
      assert(updates(1)._2.isInstanceOf[Update.ConfigurationChangeRejected])
    }
  }

  "return update [0,1] with beginAfter=[0,0]" in {
    val ps = new InMemoryKVParticipantState(participantId)
    val rt = ps.getNewRecordTime()

    for {
      _ <- ps
        .uploadPackages(archives, sourceDescription)
        .toScala
      updateTuple <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head)
    } yield {
      ps.close()
      matchPackageUpload(updateTuple, Offset(Array(0L, 1L)), archives(1), rt)
    }
  }
}
