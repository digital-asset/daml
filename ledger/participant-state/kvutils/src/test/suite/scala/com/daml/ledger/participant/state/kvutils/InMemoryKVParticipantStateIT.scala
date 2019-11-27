// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.Duration
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.backport.TimeModel
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantStateIT._
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.Assertions._
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfterEach}

import scala.compat.java8.FutureConverters._
import scala.util.Try

class InMemoryKVParticipantStateIT
    extends AsyncWordSpec
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  var ledgerId: LedgerString = _
  var ps: InMemoryKVParticipantState = _
  var rt: Timestamp = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    ledgerId = Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")
    ps = new InMemoryKVParticipantState(participantId, ledgerId)
    rt = ps.getNewRecordTime()
  }

  override protected def afterEach(): Unit = {
    ps.close()
    super.afterEach()
  }

  // TODO BH: Many of these tests for transformation from DamlLogEntry to Update better belong as
  // a KeyValueConsumptionSpec as the heart of the logic is there
  "In-memory implementation" should {

    "return initial conditions" in {
      for {
        conditions <- ps
          .getLedgerInitialConditions()
          .runWith(Sink.head)
      } yield {
        assert(conditions.ledgerId == ledgerId)
      }
    }

    "provide update after uploadPackages" in {
      for {
        result <- ps
          .uploadPackages(List(archives.head), sourceDescription, UUID.randomUUID().toString)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        matchPackageUpload(updateTuple, Offset(Array(0L, 0L)), archives.head, rt)
      }
    }

    "provide three updates after uploadPackages with three archives" in {
      val rt = ps.getNewRecordTime()
      val archive1 :: archive2 :: archive3 :: _ = archives

      for {
        result <- ps.uploadPackages(archives, sourceDescription, UUID.randomUUID().toString).toScala
        Seq(update1, update2, update3, update4) <- ps
          .stateUpdates(beginAfter = None)
          .take(4)
          .runWith(Sink.seq)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        matchPackageUpload(update1, Offset(Array(0L, 0L)), archive1, rt)
        matchPackageUpload(update2, Offset(Array(0L, 1L)), archive2, rt)
        matchPackageUpload(update3, Offset(Array(0L, 2L)), archive3, rt)
        matchPackageUploadEntryAccepted(update4, Offset(Array(0L, 3L)))
      }
    }

    "not provide update for uploadPackages with a duplicate package" in {
      val archive1 :: archive2 :: _ = archives

      for {
        _ <- ps
          .uploadPackages(List(archive1), sourceDescription, UUID.randomUUID().toString)
          .toScala
        result <- ps
          .uploadPackages(List(archive1), sourceDescription, UUID.randomUUID().toString)
          .toScala
        _ <- ps
          .uploadPackages(List(archive2), sourceDescription, UUID.randomUUID().toString)
          .toScala
        Seq(update1, update2, update3, update4, update5) <- ps
          .stateUpdates(beginAfter = None)
          .take(5)
          .runWith(Sink.seq)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        // first upload returns a package upload followed by an entry accepted
        matchPackageUpload(update1, Offset(Array(0L, 0L)), archives.head, rt)
        matchPackageUploadEntryAccepted(update2, Offset(Array(0L, 1L)))
        // second upload results in only an entry accepted as it was a duplicate
        matchPackageUploadEntryAccepted(update3, Offset(Array(1L, 0L)))
        // third upload arrives as a second package upload followed by an entry accepted:
        matchPackageUpload(update4, Offset(Array(2L, 0L)), archives(1), rt)
        matchPackageUploadEntryAccepted(update5, Offset(Array(2L, 1L)))
      }
    }

    "provide entry rejected for uploadPackages with an empty archive" in {
      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps
          .uploadPackages(List(badArchive), sourceDescription, UUID.randomUUID().toString)
          .toScala
        Seq(update1, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        matchPackageUploadEntryRejected(update1, Offset(Array(0L, 0L)))
        // there should be no package upload -- ensure next log entry is a heartbeat
        assert(update2._2.isInstanceOf[Heartbeat])
      }
    }

    "provide entry rejected for uploadPackages with a mixture of valid and invalid packages" in {
      val archive1 :: archive2 :: _ = archives
      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps
          .uploadPackages(
            List(badArchive, archive1, archive2),
            sourceDescription,
            UUID.randomUUID().toString)
          .toScala
        Seq(update1) <- ps.stateUpdates(beginAfter = None).take(1).runWith(Sink.seq)
      } yield {
        ps.close()
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        matchPackageUploadEntryRejected(update1, Offset(Array(0L, 0L)))
      }
    }

    "provide update after allocateParty" in {
      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      for {
        allocResult <- ps.allocateParty(hint, displayName, UUID.randomUUID().toString).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(
          allocResult == SubmissionResult.Acknowledged,
          "unexpected response to party allocation")
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party == hint.get)
            assert(update.displayName == displayName.get)
            assert(update.participantId == ps.participantId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(
              s"unexpected update message after a party allocation. Error : ${allocResult.description}")
        }
      }
    }

    "accept allocateParty when hint is empty" in {
      val hintNone = None
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint = None, displayName, UUID.randomUUID().toString).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party != hintNone)
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
      val hintInvalidName = Some("Alice!@")
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hintInvalidName, displayName, UUID.randomUUID().toString).toScala
        updateTuples <- ps.stateUpdates(beginAfter = None).take(1).runWith(Sink.seq)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
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
      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      for {
        result1 <- ps.allocateParty(hint, displayName, UUID.randomUUID().toString).toScala
        result2 <- ps.allocateParty(hint, displayName, UUID.randomUUID().toString).toScala
        Seq(_, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        assert(result1 == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        assert(result2 == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        update2 match {
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
      val updateResult = ps.stateUpdates(beginAfter = None).runWith(Sink.head)

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      updateResult.map {
        case (offset, _) =>
          assert(offset == Offset(Array(0L, 0L)))
      }
    }

    "reject duplicate commands" in {
      val rt = ps.getNewRecordTime()

      val updatesResult = ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      updatesResult.map { updates =>
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
    }

    "return second update with beginAfter=0" in {
      val rt = ps.getNewRecordTime()

      val updateResult =
        ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head)

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      updateResult.map {
        case (offset, update) =>
          assert(offset == Offset(Array(1L, 0L)))
          assert(update.isInstanceOf[Update.CommandRejected])
      }
    }

    "return update [0,1] with beginAfter=[0,0]" in {
      val rt = ps.getNewRecordTime()

      for {
        _ <- ps
          .uploadPackages(archives, sourceDescription, UUID.randomUUID().toString)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head)
      } yield {
        matchPackageUpload(updateTuple, Offset(Array(0L, 1L)), archives(1), rt)
      }
    }

    "correctly implements open world tx submission authorization" in {
      val rt = ps.getNewRecordTime()

      val updatesResult = ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq)

      for {
        // Submit without allocation in open world setting, expecting this to succeed.
        _ <- ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction).toScala

        // Allocate a party and try the submission again with an allocated party.
        allocResult <- ps
          .allocateParty(
            None /* no name hint, implementation decides party name */,
            Some("Somebody"),
            UUID.randomUUID().toString)
          .toScala
        _ <- assert(allocResult.isInstanceOf[SubmissionResult])
        newParty <- ps
          .stateUpdates(beginAfter = Some(Offset(Array(0L, 0L))))
          .runWith(Sink.head)
          .map(_._2.asInstanceOf[PartyAddedToParticipant].party)
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, party = newParty),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        Seq((offset1, update1), (offset2, update2), (offset3, update3)) <- updatesResult
      } yield {
        assert(offset1 == Offset(Array(0, 0)))
        assert(update1.isInstanceOf[Update.TransactionAccepted])

        assert(offset2 == Offset(Array(1, 0)))
        assert(update2.isInstanceOf[Update.PartyAddedToParticipant])

        assert(offset3 == Offset(Array(2, 0)))
        assert(update3.isInstanceOf[Update.TransactionAccepted])
      }
    }

    "correctly implements closed world tx submission authorization" in {
      val updatesResult = ps.stateUpdates(beginAfter = None).take(4).runWith(Sink.seq)

      for {
        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        _ <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = "test1",
            config = lic.config.copy(
              generation = lic.config.generation + 1,
              openWorld = false,
            )
          )
          .toScala

        // Submit without allocation in closed world setting.
        _ <- ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction).toScala

        // Allocate a party and try the submission again with an allocated party.
        allocResult <- ps
          .allocateParty(
            None /* no name hint, implementation decides party name */,
            Some("Somebody"),
            UUID.randomUUID().toString)
          .toScala
        _ <- assert(allocResult.isInstanceOf[SubmissionResult])
        //get the new party off state updates
        newParty <- ps
          .stateUpdates(beginAfter = Some(Offset(Array(1L, 0L))))
          .runWith(Sink.head)
          .map(_._2.asInstanceOf[PartyAddedToParticipant].party)
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, party = newParty),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        updates <- updatesResult
      } yield {
        def takeUpdate(n: Int) = {
          val (offset, update) = updates(n)
          assert(offset == Offset(Array(n.toLong, 0L)))
          update
        }

        assert(takeUpdate(0).isInstanceOf[Update.ConfigurationChanged])

        assert(
          takeUpdate(1)
            .asInstanceOf[Update.CommandRejected]
            .reason == RejectionReason.PartyNotKnownOnLedger)

        assert(takeUpdate(2).isInstanceOf[Update.PartyAddedToParticipant])
        assert(takeUpdate(3).isInstanceOf[Update.TransactionAccepted])
      }
    }

    "allow an administrator to submit new configuration" in {
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
              generation = lic.config.generation + 1,
              timeModel = TimeModel(
                Duration.ofSeconds(123),
                Duration.ofSeconds(123),
                Duration.ofSeconds(123)).get
            )
          )
          .toScala

        Seq((_, update1), (_, update2)) <- ps.stateUpdates(None).take(2).runWith(Sink.seq)
      } yield {
        // The first submission should change the config.
        val newConfig = update1.asInstanceOf[Update.ConfigurationChanged]
        assert(newConfig.newConfiguration != lic.config)

        // The second submission should get rejected.
        assert(update2.isInstanceOf[Update.ConfigurationChangeRejected])
      }
    }
  }
}

object InMemoryKVParticipantStateIT {
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
      rt: Timestamp
  ): Assertion = updateTuple match {
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
      assert(update.reason contains "rejected as invalid")
    case _ => fail("did not find expected upload entry rejected")
  }
}
