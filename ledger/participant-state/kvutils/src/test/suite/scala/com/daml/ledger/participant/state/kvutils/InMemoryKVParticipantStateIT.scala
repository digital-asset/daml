// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.Duration
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantStateIT._
import com.daml.ledger.participant.state.v1.Update.{PartyAddedToParticipant, PublicPackageUploaded}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{InsertOrdSet, ImmArray}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.Assertions._
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfterEach}

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._
import scala.util.Try
import scala.collection.immutable.SortedMap

class InMemoryKVParticipantStateIT
    extends AsyncWordSpec
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  var ledgerId: LedgerString = _
  var ps: InMemoryKVParticipantState = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    ledgerId = Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")
    ps = new InMemoryKVParticipantState(participantId, ledgerId)
  }

  override protected def afterEach(): Unit = {
    ps.close()
    super.afterEach()
  }

  private def allocateParty(hint: String): Future[Party] = {
    ps.allocateParty(Some(hint), None).toScala.flatMap {
      case PartyAllocationResult.Ok(details) => Future.successful(details.party)
      case err => Future.failed(new RuntimeException("failed to allocate party: $err"))
    }
  }

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
      val rt = ps.getNewRecordTime()

      for {
        result <- ps.uploadPackages(List(archives.head), sourceDescription).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(result == UploadPackagesResult.Ok, "unexpected response to party allocation")
        matchPackageUpload(updateTuple, Offset(Array(0L, 0L)), archives.head, rt)
      }
    }

    "provide two updates after uploadPackages with two archives" in {
      val rt = ps.getNewRecordTime()
      val archive1 :: archive2 :: _ = archives

      for {
        result <- ps.uploadPackages(archives, sourceDescription).toScala
        Seq(update1, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        assert(result == UploadPackagesResult.Ok, "unexpected response to party allocation")
        matchPackageUpload(update1, Offset(Array(0L, 0L)), archive1, rt)
        matchPackageUpload(update2, Offset(Array(0L, 1L)), archive2, rt)
      }
    }

    "remove duplicate package from update after uploadPackages" in {
      val rt = ps.getNewRecordTime()
      val archive1 :: archive2 :: _ = archives

      for {
        _ <- ps.uploadPackages(List(archive1), sourceDescription).toScala
        result <- ps.uploadPackages(List(archive1), sourceDescription).toScala
        _ <- ps.uploadPackages(List(archive2), sourceDescription).toScala
        Seq(update1, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        assert(result == UploadPackagesResult.Ok, "unexpected response to party allocation")
        // first upload arrives as head update:
        matchPackageUpload(update1, Offset(Array(0L, 0L)), archive1, rt)
        // second upload results in no update because it was a duplicate
        // third upload arrives as a second update:
        matchPackageUpload(update2, Offset(Array(2L, 0L)), archive2, rt)
      }
    }

    "reject uploadPackages when archive is empty" in {

      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      for {
        result <- ps.uploadPackages(List(badArchive), sourceDescription).toScala
      } yield {
        result match {
          case UploadPackagesResult.InvalidPackage(_) =>
            succeed
          case _ =>
            fail("unexpected response to package upload")
        }
      }
    }

    "provide update after allocateParty" in {
      val rt = ps.getNewRecordTime()

      val hint = Some("Alice")
      val displayName = Some("Alice Cooper")

      for {
        allocResult <- ps.allocateParty(hint, displayName).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
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

      for {
        result <- ps.allocateParty(hint, displayName).toScala
      } yield {
        result match {
          case PartyAllocationResult.Ok(_) =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
      }
    }

    "reject allocateParty when hint contains invalid string for a party" in {
      val hint = Some("Alice!@")
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint, displayName).toScala
      } yield {
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

      for {
        _ <- ps.allocateParty(hint, displayName).toScala
        result <- ps.allocateParty(hint, displayName).toScala
      } yield {
        result match {
          case PartyAllocationResult.AlreadyExists =>
            succeed
          case _ =>
            fail("unexpected response to party allocation")
        }
      }
    }

    "provide update after transaction submission" in {
      val rt = ps.getNewRecordTime()
      for {
        alice <- allocateParty("alice")
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        update <- ps.stateUpdates(beginAfter = None).drop(1).runWith(Sink.head)
      } yield {
        assert(update._1 == Offset(Array(1L, 0L)))
      }
    }

    "reject duplicate commands" in {
      val rt = ps.getNewRecordTime()

      for {
        alice <- allocateParty("alice")
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        updates <- ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq)
      } yield {
        val (offset0, update0) = updates(0)
        assert(offset0 == Offset(Array(0L, 0L)))
        assert(update0.isInstanceOf[Update.PartyAddedToParticipant])

        val (offset1, update1) = updates(1)
        assert(offset1 == Offset(Array(1L, 0L)))
        assert(update1.isInstanceOf[Update.TransactionAccepted])

        val (offset2, update2) = updates(2)
        assert(offset2 == Offset(Array(2L, 0L)))
      }
    }

    "return second update with beginAfter=0" in {
      val rt = ps.getNewRecordTime()
      for {
        alice <- allocateParty("alice") // offset now at [1,0]
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        offsetAndUpdate <- ps
          .stateUpdates(beginAfter = Some(Offset(Array(1L, 0L))))
          .runWith(Sink.head)
      } yield {
        val (offset, update) = offsetAndUpdate
        assert(offset == Offset(Array(2L, 0L)))
        assert(update.isInstanceOf[Update.CommandRejected])
      }
    }

    "return update [0,1] with beginAfter=[0,0]" in {
      val rt = ps.getNewRecordTime()

      for {
        _ <- ps
          .uploadPackages(archives, sourceDescription)
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = Some(Offset(Array(0L, 0L)))).runWith(Sink.head)
      } yield {
        matchPackageUpload(updateTuple, Offset(Array(0L, 1L)), archives(1), rt)
      }
    }

    "correctly implements tx submission authorization" in {
      val rt = ps.getNewRecordTime()

      val updatesResult = ps.stateUpdates(beginAfter = None).take(4).runWith(Sink.seq)
      val unallocatedParty = Ref.Party.assertFromString("nobody")

      for {
        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        _ <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = "test1",
            config = lic.config.copy(
              generation = lic.config.generation + 1,
            )
          )
          .toScala

        // Submit without allocation
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, unallocatedParty),
            transactionMeta(rt),
            emptyTransaction)
          .toScala

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

        Seq((offset1, update1), (offset2, update2), (offset3, update3), (offset4, update4)) <- ps
          .stateUpdates(beginAfter = None)
          .take(4)
          .runWith(Sink.seq)

      } yield {
        assert(update1.isInstanceOf[Update.ConfigurationChanged])
        assert(offset1 == Offset(Array(0L, 0L)))
        assert(
          update2
            .asInstanceOf[Update.CommandRejected]
            .reason == RejectionReason.PartyNotKnownOnLedger)
        assert(offset2 == Offset(Array(1L, 0L)))
        assert(update3.isInstanceOf[Update.PartyAddedToParticipant])
        assert(offset3 == Offset(Array(2L, 0L)))
        assert(update4.isInstanceOf[Update.TransactionAccepted])
        assert(offset4 == Offset(Array(3L, 0L)))
      }
    }

    "allow an administrator to submit new configuration" in {
      val rt = ps.getNewRecordTime()

      for {
        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        // Submit an initial configuration change
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
    GenTransaction(SortedMap.empty, ImmArray.empty, Some(InsertOrdSet.empty))

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
}
