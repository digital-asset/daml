// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.Duration

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.Update.{PartyAddedToParticipant, PublicPackageUploaded}
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
import scala.concurrent.Future
import scala.concurrent.duration._

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

  private def submitterInfo(rt: Timestamp, party: Ref.Party) = SubmitterInfo(
    submitter = party,
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

  def allocateParty(ps: InMemoryKVParticipantState, hint: String): Future[Party] = {
    ps.allocateParty(Some(hint), None).toScala.flatMap {
      case PartyAllocationResult.Ok(details) => Future.successful(details.party)
      case err => Future.failed(new RuntimeException("failed to allocate party: $err"))
    }
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
      val rt = ps.getNewRecordTime
      for {
        result <- ps
          .uploadPackages(List(archives.head), sourceDescription)
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
        matchPackageUpload(updateTuple, Offset(Array(0L, 0L)), archives.head, rt)
      }
    }

    "provide two updates after uploadPackages with two archives" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
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
        matchPackageUpload(updateTuples.head, Offset(Array(0L, 0L)), archives.head, rt)
        matchPackageUpload(updateTuples(1), Offset(Array(0L, 1L)), archives(1), rt)
      }
    }

    "remove duplicate package from update after uploadPackages" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
      for {
        _ <- ps
          .uploadPackages(List(archives.head), sourceDescription)
          .toScala
        result <- ps
          .uploadPackages(List(archives.head), sourceDescription)
          .toScala
        _ <- ps
          .uploadPackages(List(archives(1)), sourceDescription)
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
        matchPackageUpload(updateTuples.head, Offset(Array(0L, 0L)), archives.head, rt)
        // second upload results in no update because it was a duplicate
        // third upload arrives as a second update:
        matchPackageUpload(updateTuples(1), Offset(Array(2L, 0L)), archives(1), rt)
      }
    }

    "reject uploadPackages when archive is empty" in {
      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      val ps = new InMemoryKVParticipantState(participantId)
      for {
        result <- ps
          .uploadPackages(List(badArchive), sourceDescription)
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
      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
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
      val hint = None
      val displayName = Some("Alice Cooper")

      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime

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
      val hint = Some("Alice!@")
      val displayName = Some("Alice Cooper")

      val ps = new InMemoryKVParticipantState(participantId)
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

      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      val ps = new InMemoryKVParticipantState(participantId)
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
      val rt = ps.getNewRecordTime
      for {
        alice <- allocateParty(ps, "alice")
        _ <- ps.submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction).toScala
        update <- ps.stateUpdates(beginAfter = None).drop(1).runWith(Sink.head)
      } yield {
        ps.close()
        assert(update._1 == Offset(Array(1L, 0L)))
      }
    }

    "reject duplicate commands" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
      for {
        alice <- allocateParty(ps, "alice")
        _ <- ps.submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction).toScala
        _ <- ps.submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction).toScala

        updates <- ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq)
      } yield {
        ps.close()

        val (offset0, update0) = updates(0)
        assert(offset0 == Offset(Array(0L, 0L)))
        assert(update0.isInstanceOf[Update.PartyAddedToParticipant])

        val (offset1, update1) = updates(1)
        assert(offset1 == Offset(Array(1L, 0L)))
        assert(update1.isInstanceOf[Update.TransactionAccepted])

        val (offset2, update2) = updates(2)
        assert(offset2 == Offset(Array(2L, 0L)))
        assert(update2.isInstanceOf[Update.CommandRejected])
        assert(
          update2
            .asInstanceOf[Update.CommandRejected]
            .reason == RejectionReason.DuplicateCommand)
      }
    }

    "return second update with beginAfter=0" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
      for {
        alice <- allocateParty(ps, "alice") // offset now at [1,0]
        _ <- ps.submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction).toScala
        _ <- ps.submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction).toScala
        offsetAndUpdate <- ps.stateUpdates(beginAfter = Some(Offset(Array(1L, 0L)))).runWith(Sink.head)
      } yield {
        val (offset, update) = offsetAndUpdate
        ps.close()
        assert(offset == Offset(Array(2L, 0L)))
        assert(update.isInstanceOf[Update.CommandRejected])
      }
    }

    "correctly implements tx submission authorization" in {
      val ps = new InMemoryKVParticipantState(participantId)
      val rt = ps.getNewRecordTime
      val unallocatedParty = Ref.Party.assertFromString("nobody")

      for {
        // Submit without allocation
        _ <- ps.submitTransaction(submitterInfo(rt, unallocatedParty), transactionMeta(rt), emptyTransaction).toScala

        // Allocate a party and try the submission again with an allocated party.
        allocResult <- ps
          .allocateParty(
            None /* no name hint, implementation decides party name */,
            Some("Somebody"))
          .toScala
        _ <- assert(allocResult.isInstanceOf[PartyAllocationResult.Ok])
        _ <- ps
          .submitTransaction(
            submitterInfo(
              rt,
              party = allocResult.asInstanceOf[PartyAllocationResult.Ok].result.party),
            transactionMeta(rt),
            emptyTransaction)
          .toScala

        updates <- ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq)
      } yield {
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
    }
  }

  "can submit new configuration" in {
    val ps = new InMemoryKVParticipantState(participantId)
    val rt = ps.getNewRecordTime
    for {
      lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

      // Submit a configuration change that flips the "open world" flag.
      _ <- ps
        .submitConfiguration(
          maxRecordTime = rt.addMicros(1000000),
          submissionId = "test1",
          config = lic.config.copy(
            generation = lic.config.generation + 1,
          ))
        .toScala

      // Submit another configuration change that uses stale "current config".
      _ <- ps
        .submitConfiguration(
          maxRecordTime = rt.addMicros(1000000),
          submissionId = "test2",
          config = lic.config.copy(
            timeModel = TimeModelImpl(
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
    val rt = ps.getNewRecordTime
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
