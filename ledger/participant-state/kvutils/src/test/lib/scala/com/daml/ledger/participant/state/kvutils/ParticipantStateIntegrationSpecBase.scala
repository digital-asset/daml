// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase._
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, InsertOrdSet, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.Assertions._
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfterEach}

import scala.collection.immutable.SortedMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

abstract class ParticipantStateIntegrationSpecBase
    extends AsyncWordSpec
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  var ledgerId: LedgerString = _
  var ps: ReadService with WriteService with AutoCloseable = _
  var rt: Timestamp = _

  def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ReadService with WriteService with AutoCloseable

  def currentRecordTime(): Timestamp

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    ledgerId = Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")
    ps = participantStateFactory(participantId, ledgerId)
    rt = currentRecordTime()
  }

  override protected def afterEach(): Unit = {
    ps.close()
    super.afterEach()
  }

  private val alice = Ref.Party.assertFromString("alice")
  private def randomLedgerString(): Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  // TODO(BH): Many of these tests for transformation from DamlLogEntry to Update better belong as
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
      val submissionId = randomLedgerString()
      for {
        _ <- ps.uploadPackages(submissionId, List(archives.head), sourceDescription).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        matchPackageUpload(
          updateTuple,
          submissionId,
          Offset(Array(0L, 0L)),
          List(archives.head),
          rt)
      }
    }

    "provide two updates after uploadPackages with two archives" in {
      val submissionId = randomLedgerString()
      for {
        _ <- ps.uploadPackages(submissionId, archives, sourceDescription).toScala
        update1 <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        matchPackageUpload(update1, submissionId, Offset(Array(0L, 0L)), archives, rt)
      }
    }

    "remove duplicate package from update after uploadPackages" in {
      val archive1 :: archive2 :: _ = archives
      val (subId1, subId2, subId3) =
        (randomLedgerString(), randomLedgerString(), randomLedgerString())

      for {
        _ <- ps.uploadPackages(subId1, List(archive1), sourceDescription).toScala
        _ <- ps.uploadPackages(subId2, List(archive1), sourceDescription).toScala
        _ <- ps.uploadPackages(subId3, List(archive2), sourceDescription).toScala
        Seq(update1, update2, update3) <- ps
          .stateUpdates(beginAfter = None)
          .take(3)
          .runWith(Sink.seq)
      } yield {
        // first upload arrives as head update:
        matchPackageUpload(update1, subId1, Offset(Array(0L, 0L)), List(archive1), rt)
        matchPackageUpload(update2, subId2, Offset(Array(1L, 0L)), List(), rt)
        matchPackageUpload(update3, subId3, Offset(Array(2L, 0L)), List(archive2), rt)
      }
    }

    "reject uploadPackages when archive is empty" in {

      val badArchive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build

      val submissionId = randomLedgerString()

      for {
        _ <- ps.uploadPackages(submissionId, List(badArchive), sourceDescription).toScala
        updateTuple <- ps
          .stateUpdates(beginAfter = None)
          .idleTimeout(DefaultIdleTimeout)
          .runWith(Sink.head)
      } yield {
        updateTuple match {
          case (offset: Offset, update: PublicPackageUploadRejected) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.submissionId == submissionId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(s"unexpected update message after package upload: $updateTuple")
        }
      }
    }

    "reject duplicate submission in uploadPackage" in {
      val submissionIds = (randomLedgerString(), randomLedgerString())
      val archive1 :: archive2 :: _ = archives

      for {
        result1 <- ps.uploadPackages(submissionIds._1, List(archive1), sourceDescription).toScala
        result2 <- ps.uploadPackages(submissionIds._1, List(archive1), sourceDescription).toScala
        result3 <- ps.uploadPackages(submissionIds._2, List(archive2), sourceDescription).toScala
        // second submission is a duplicate, it fails silently
        Seq(_, update2) <- ps
          .stateUpdates(beginAfter = None)
          .take(2)
          .runWith(Sink.seq)
      } yield {
        List(result1, result2, result3).map(
          result =>
            assert(result == SubmissionResult.Acknowledged, "unexpected response to package upload")
        )
        update2 match {
          case (offset: Offset, update: PublicPackageUpload) =>
            assert(offset == Offset(Array(2L, 0L)))
            assert(update.submissionId.contains(submissionIds._2))
          case _ =>
            fail(
              "unexpected update message after a package upload.  Error : " + result2.description)
        }
      }
    }

    "provide update after allocateParty" in {
      val partyHint = Ref.Party.assertFromString("Alice")
      val displayName = "Alice Cooper"

      for {
        allocResult <- ps
          .allocateParty(Some(partyHint), Some(displayName), randomLedgerString())
          .toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(
          allocResult == SubmissionResult.Acknowledged,
          s"unexpected response to party allocation: $allocResult")
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party == partyHint)
            assert(update.displayName == displayName)
            assert(update.participantId == participantId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(s"unexpected update message after a party allocation: $updateTuple")
        }
      }
    }

    "accept allocateParty when hint is empty" in {
      val displayName = Some("Alice Cooper")

      for {
        result <- ps.allocateParty(hint = None, displayName, randomLedgerString()).toScala
        updateTuple <- ps.stateUpdates(beginAfter = None).runWith(Sink.head)
      } yield {
        assert(result == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        updateTuple match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(0L, 0L)))
            assert(update.party.nonEmpty)
            assert(update.displayName == displayName.get)
            assert(update.participantId == participantId)
            assert(update.recordTime >= rt)
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result.description)
        }

      }
    }

    "reject duplicate submission in allocateParty" in {
      val hints =
        (Some(Ref.Party.assertFromString("Alice")), Some(Ref.Party.assertFromString("Bob")))
      val displayNames = (Some("Alice Cooper"), Some("Bob de Boumaa"))

      val submissionIds = (randomLedgerString(), randomLedgerString())

      for {
        result1 <- ps.allocateParty(hints._1, displayNames._1, submissionIds._1).toScala
        result2 <- ps.allocateParty(hints._2, displayNames._2, submissionIds._1).toScala
        result3 <- ps.allocateParty(hints._2, displayNames._2, submissionIds._2).toScala
        // second submission is a duplicate, it fails silently
        Seq(_, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        List(result1, result2, result3).map(
          result =>
            assert(
              result == SubmissionResult.Acknowledged,
              "unexpected response to party allocation")
        )
        update2 match {
          case (offset: Offset, update: PartyAddedToParticipant) =>
            assert(offset == Offset(Array(2L, 0L)))
            assert(update.submissionId.contains(submissionIds._2))
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result2.description)
        }
      }
    }

    "reject duplicate party in allocateParty" in {
      val hint = Some(Ref.Party.assertFromString("Alice"))
      val displayName = Some("Alice Cooper")

      for {
        result1 <- ps.allocateParty(hint, displayName, randomLedgerString()).toScala
        result2 <- ps.allocateParty(hint, displayName, randomLedgerString()).toScala
        Seq(_, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        assert(result1 == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        assert(result2 == SubmissionResult.Acknowledged, "unexpected response to party allocation")
        update2 match {
          case (offset: Offset, update: PartyAllocationRejected) =>
            assert(offset == Offset(Array(1L, 0L)))
            assert(update.rejectionReason equalsIgnoreCase "Party already exists")
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result2.description)
        }
      }
    }

    "provide update after transaction submission" in {
      val rt = currentRecordTime()
      for {
        _ <- ps.allocateParty(hint = Some(alice), None, randomLedgerString()).toScala
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
          .toScala
        update <- ps.stateUpdates(beginAfter = None).drop(1).runWith(Sink.head)
      } yield {
        assert(update._1 == Offset(Array(1L, 0L)))
      }
    }

    "reject duplicate commands" in {
      val rt = currentRecordTime()
      val commandIds = ("X1", "X2")

      for {
        _ <- ps.allocateParty(hint = Some(alice), None, randomLedgerString()).toScala
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, alice, commandIds._1),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, alice, commandIds._1),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        _ <- ps
          .submitTransaction(
            submitterInfo(rt, alice, commandIds._2),
            transactionMeta(rt),
            emptyTransaction)
          .toScala
        updates <- ps.stateUpdates(beginAfter = None).take(3).runWith(Sink.seq)
      } yield {
        val Seq(update0, update1, update2) = updates
        assert(update0._1 == Offset(Array(0L, 0L)))
        assert(update0._2.isInstanceOf[Update.PartyAddedToParticipant])

        matchTransaction(update1, commandIds._1, Offset(Array(1L, 0L)), rt)
        matchTransaction(update2, commandIds._2, Offset(Array(3L, 0L)), rt)
      }
    }

    "return second update with beginAfter=0" in {
      val rt = currentRecordTime()
      for {
        _ <- ps
          .allocateParty(hint = Some(alice), None, randomLedgerString())
          .toScala // offset now at [1,0]
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice, "X1"), transactionMeta(rt), emptyTransaction)
          .toScala
        _ <- ps
          .submitTransaction(submitterInfo(rt, alice, "X2"), transactionMeta(rt), emptyTransaction)
          .toScala
        offsetAndUpdate <- ps
          .stateUpdates(beginAfter = Some(Offset(Array(1L, 0L))))
          .runWith(Sink.head)
      } yield {
        val (offset, update) = offsetAndUpdate
        assert(offset == Offset(Array(2L, 0L)))
        assert(update.isInstanceOf[Update.TransactionAccepted])
      }
    }

    "correctly implements tx submission authorization" in {
      val rt = currentRecordTime()

      val unallocatedParty = Ref.Party.assertFromString("nobody")

      for {
        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        _ <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = randomLedgerString(),
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
            Some("Somebody"),
            randomLedgerString())
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
      val rt = currentRecordTime()

      for {
        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        // Submit an initial configuration change
        _ <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = randomLedgerString(),
            config = lic.config.copy(
              generation = lic.config.generation + 1,
            ))
          .toScala

        // Submit another configuration change that uses stale "current config".
        _ <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = randomLedgerString(),
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

    "reject duplicate submission in new configuration" in {
      val submissionIds = (randomLedgerString(), randomLedgerString())

      for {

        lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

        // Submit an initial configuration change
        result1 <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(1000000),
            submissionId = submissionIds._1,
            config = lic.config.copy(
              generation = lic.config.generation + 1,
            ))
          .toScala
        result2 <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(2000000),
            submissionId = submissionIds._1,
            config = lic.config.copy(
              generation = lic.config.generation + 2,
            ))
          .toScala
        result3 <- ps
          .submitConfiguration(
            maxRecordTime = rt.addMicros(2000000),
            submissionId = submissionIds._2,
            config = lic.config.copy(
              generation = lic.config.generation + 2,
            ))
          .toScala
        // second submission is a duplicate, it fails silently
        Seq(_, update2) <- ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq)
      } yield {
        List(result1, result2, result3).map(
          result =>
            assert(
              result == SubmissionResult.Acknowledged,
              "unexpected response to configuration change"))
        update2 match {
          case (offset: Offset, update: ConfigurationChanged) =>
            assert(offset == Offset(Array(2L, 0L)))
            assert(update.submissionId == submissionIds._2)
          case _ =>
            fail(
              "unexpected update message after a party allocation.  Error : " + result2.description)
        }
      }
    }
  }
}

object ParticipantStateIntegrationSpecBase {
  private val DefaultIdleTimeout = FiniteDuration(5, TimeUnit.SECONDS)
  private val emptyTransaction: SubmittedTransaction =
    GenTransaction(SortedMap.empty, ImmArray.empty, Some(InsertOrdSet.empty))

  private val participantId: ParticipantId =
    Ref.LedgerString.assertFromString("in-memory-participant")
  private val sourceDescription = Some("provided by test")

  private val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }
  private val archives =
    darReader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar"))).get.all

  private def submitterInfo(rt: Timestamp, party: Ref.Party, commandId: String = "X") =
    SubmitterInfo(
      submitter = party,
      applicationId = Ref.LedgerString.assertFromString("tests"),
      commandId = Ref.LedgerString.assertFromString(commandId),
      maxRecordTime = rt.addMicros(Duration.ofSeconds(10).toNanos / 1000)
    )

  private def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests"))
  )

  private def matchPackageUpload(
      updateTuple: (Offset, Update),
      submissionId: SubmissionId,
      givenOffset: Offset,
      expectedArchives: List[DamlLf.Archive],
      rt: Timestamp
  ): Assertion = updateTuple match {
    case (offset: Offset, update: PublicPackageUpload) =>
      assert(update.submissionId.contains(submissionId))
      assert(offset == givenOffset)
      assert(update.archives.map(_.getHash).toSet == expectedArchives.map(_.getHash).toSet)
      assert(update.sourceDescription == sourceDescription)
      assert(update.recordTime >= rt)
    case _ => fail("unexpected update message after a package upload: $updateTuple")
  }

  private def matchTransaction(
      updateTuple: (Offset, Update),
      commandId: String,
      givenOffset: Offset,
      rt: Timestamp): Assertion = updateTuple match {
    case (offset: Offset, update: TransactionAccepted) =>
      update.optSubmitterInfo match {
        case Some(info) =>
          assert(info.commandId == commandId)
        case _ =>
          fail("missing submitter info")
      }
      assert(offset == givenOffset)
      assert(update.recordTime >= rt)
    case _ => fail("unexpected update message after a transaction submission: $updateTuple")
  }
}
