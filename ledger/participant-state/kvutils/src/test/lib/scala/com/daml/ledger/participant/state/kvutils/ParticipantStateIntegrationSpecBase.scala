// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File
import java.time.{Clock, Duration}
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase._
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, InsertOrdSet, Ref}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.common.LedgerIdMismatchException
import com.digitalasset.resources.ResourceOwner
import org.scalatest.Inside._
import org.scalatest.Matchers._
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfterEach}

import scala.collection.immutable.HashMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

//noinspection DuplicatedCode
abstract class ParticipantStateIntegrationSpecBase(implementationName: String)
    extends AsyncWordSpec
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  // Can be used by [[participantStateFactory]] to get a stable ID throughout the test.
  // For example, for initializing a database.
  private var testId: String = _

  private var rt: Timestamp = _

  // This can be overriden by tests for ledgers that don't start at 0.
  protected val startIndex: Long = 0

  // This can be overriden by tests for in-memory or otherwise ephemeral ledgers.
  protected val isPersistent: Boolean = true

  protected def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState]

  private def participantState: ResourceOwner[ParticipantState] =
    newParticipantState(newLedgerId())

  private def newParticipantState(): ResourceOwner[ParticipantState] =
    newLoggingContext { implicit logCtx =>
      participantStateFactory(None, participantId, testId)
    }

  private def newParticipantState(ledgerId: LedgerId): ResourceOwner[ParticipantState] =
    newLoggingContext { implicit logCtx =>
      participantStateFactory(Some(ledgerId), participantId, testId)
    }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testId = UUID.randomUUID().toString
    rt = Timestamp.assertFromInstant(Clock.systemUTC().instant())
  }

  // TODO(BH): Many of these tests for transformation from DamlLogEntry to Update better belong as
  // a KeyValueConsumptionSpec as the heart of the logic is there

  implementationName should {
    "return initial conditions" in {
      val ledgerId = newLedgerId()
      newParticipantState(ledgerId).use { ps =>
        for {
          conditions <- ps
            .getLedgerInitialConditions()
            .runWith(Sink.head)
        } yield {
          conditions.ledgerId should be(ledgerId)
        }
      }
    }

    "uploadPackages" should {
      "provide an update" in participantState.use { ps =>
        val submissionId = newSubmissionId()
        for {
          result <- ps.uploadPackages(submissionId, List(archives.head), sourceDescription).toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(0, 0))
          update.recordTime should be >= rt
          matchPackageUpload(update, submissionId, List(archives.head))
        }
      }

      "provide two updates when uploading two archives" in participantState.use { ps =>
        val submissionId = newSubmissionId()
        for {
          result <- ps.uploadPackages(submissionId, archives, sourceDescription).toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(0, 0))
          update.recordTime should be >= rt
          matchPackageUpload(update, submissionId, archives)
        }
      }

      "remove a duplicate package from the update" in participantState.use { ps =>
        val archive1 :: archive2 :: _ = archives
        val (subId1, subId2, subId3) =
          (newSubmissionId(), newSubmissionId(), newSubmissionId())

        for {
          result1 <- ps.uploadPackages(subId1, List(archive1), sourceDescription).toScala
          result2 <- ps.uploadPackages(subId2, List(archive1), sourceDescription).toScala
          result3 <- ps.uploadPackages(subId3, List(archive2), sourceDescription).toScala
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          Seq((offset1, update1), (offset2, update2), (offset3, update3)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(3)
            .runWith(Sink.seq)
          updates = Seq(update1, update2, update3)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt
          // first upload arrives as head update:
          offset1 should be(theOffset(0, 0))
          matchPackageUpload(update1, subId1, List(archive1))
          offset2 should be(theOffset(1, 0))
          matchPackageUpload(update2, subId2, List())
          offset3 should be(theOffset(2, 0))
          matchPackageUpload(update3, subId3, List(archive2))
        }
      }

      "reject an empty archive" in participantState.use { ps =>
        val badArchive = DamlLf.Archive.newBuilder
          .setHash("asdf")
          .build

        val submissionId = newSubmissionId()

        for {
          result <- ps.uploadPackages(submissionId, List(badArchive), sourceDescription).toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(0, 0))
          update.recordTime should be >= rt
          inside(update) {
            case PublicPackageUploadRejected(actualSubmissionId, _, _) =>
              actualSubmissionId should be(submissionId)
          }
        }
      }

      "reject a duplicate submission" in participantState.use { ps =>
        val submissionIds = (newSubmissionId(), newSubmissionId())
        val archive1 :: archive2 :: _ = archives

        for {
          result1 <- ps.uploadPackages(submissionIds._1, List(archive1), sourceDescription).toScala
          result2 <- ps.uploadPackages(submissionIds._1, List(archive1), sourceDescription).toScala
          result3 <- ps.uploadPackages(submissionIds._2, List(archive2), sourceDescription).toScala
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          // second submission is a duplicate, it fails silently
          Seq(_, (offset2, update2)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(2)
            .runWith(Sink.seq)
        } yield {
          offset2 should be(theOffset(2, 0))
          update2.recordTime should be >= rt
          inside(update2) {
            case PublicPackageUpload(_, _, _, Some(submissionId)) =>
              submissionId should be(submissionIds._2)
          }
        }
      }
    }

    "allocateParty" should {
      "provide an update" in participantState.use { ps =>
        val partyHint = Ref.Party.assertFromString("Alice")
        val displayName = "Alice Cooper"

        for {
          result <- ps
            .allocateParty(Some(partyHint), Some(displayName), newSubmissionId())
            .toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(0, 0))
          update.recordTime should be >= rt
          inside(update) {
            case PartyAddedToParticipant(party, actualDisplayName, actualParticipantId, _, _) =>
              party should be(partyHint)
              actualDisplayName should be(displayName)
              actualParticipantId should be(participantId)
          }
        }
      }

      "accept when the hint is empty" in participantState.use { ps =>
        val displayName = "Alice Cooper"

        for {
          result <- ps.allocateParty(hint = None, Some(displayName), newSubmissionId()).toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(0, 0))
          update.recordTime should be >= rt
          inside(update) {
            case PartyAddedToParticipant(party, actualDisplayName, actualParticipantId, _, _) =>
              party should not be empty
              actualDisplayName should be(displayName)
              actualParticipantId should be(participantId)
          }
        }
      }

      "reject a duplicate submission" in participantState.use { ps =>
        val hints =
          (Some(Ref.Party.assertFromString("Alice")), Some(Ref.Party.assertFromString("Bob")))
        val displayNames = ("Alice Cooper", "Bob de Boumaa")

        val submissionIds = (newSubmissionId(), newSubmissionId())

        for {
          result1 <- ps.allocateParty(hints._1, Some(displayNames._1), submissionIds._1).toScala
          result2 <- ps.allocateParty(hints._2, Some(displayNames._2), submissionIds._1).toScala
          result3 <- ps.allocateParty(hints._2, Some(displayNames._2), submissionIds._2).toScala
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          // second submission is a duplicate, it fails silently
          Seq(_, (offset2, update2)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(2)
            .runWith(Sink.seq)
        } yield {
          offset2 should be(theOffset(2, 0))
          update2.recordTime should be >= rt
          inside(update2) {
            case PartyAddedToParticipant(_, displayName, _, _, Some(submissionId)) =>
              displayName should be(displayNames._2)
              submissionId should be(submissionIds._2)
          }
        }
      }

      "reject a duplicate party" in participantState.use { ps =>
        val hint = Some(Ref.Party.assertFromString("Alice"))
        val displayName = Some("Alice Cooper")

        for {
          result1 <- ps.allocateParty(hint, displayName, newSubmissionId()).toScala
          result2 <- ps.allocateParty(hint, displayName, newSubmissionId()).toScala
          results = Seq(result1, result2)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          Seq(_, (offset2, update2)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(2)
            .runWith(Sink.seq)
        } yield {
          offset2 should be(theOffset(1, 0))
          update2.recordTime should be >= rt
          inside(update2) {
            case PartyAllocationRejected(_, _, _, rejectionReason) =>
              rejectionReason should be("Party already exists")
          }
        }
      }
    }

    "submitTransaction" should {
      "provide an update after a transaction submission" in participantState.use { ps =>
        for {
          _ <- ps.allocateParty(hint = Some(alice), None, newSubmissionId()).toScala
          _ <- ps
            .submitTransaction(submitterInfo(rt, alice), transactionMeta(rt), emptyTransaction)
            .toScala
          (offset, _) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .drop(1)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(1, 0))
        }
      }

      "reject duplicate commands" in participantState.use { ps =>
        val commandIds = ("X1", "X2")

        for {
          result1 <- ps.allocateParty(hint = Some(alice), None, newSubmissionId()).toScala
          result2 <- ps
            .submitTransaction(
              submitterInfo(rt, alice, commandIds._1),
              transactionMeta(rt),
              emptyTransaction,
            )
            .toScala
          result3 <- ps
            .submitTransaction(
              submitterInfo(rt, alice, commandIds._1),
              transactionMeta(rt),
              emptyTransaction,
            )
            .toScala
          result4 <- ps
            .submitTransaction(
              submitterInfo(rt, alice, commandIds._2),
              transactionMeta(rt),
              emptyTransaction,
            )
            .toScala
          results = Seq(result1, result2, result3, result4)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          Seq((offset1, update1), (offset2, update2), (offset3, update3)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(3)
            .runWith(Sink.seq)
          updates = Seq(update1, update2, update3)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt

          offset1 should be(theOffset(0, 0))
          update1 should be(a[PartyAddedToParticipant])

          offset2 should be(theOffset(1, 0))
          matchTransaction(update2, commandIds._1)

          offset3 should be(theOffset(3, 0))
          matchTransaction(update3, commandIds._2)
        }
      }

      "return the third update with beginAfter=1" in participantState.use { ps =>
        for {
          result1 <- ps
            .allocateParty(hint = Some(alice), None, newSubmissionId())
            .toScala // offset now at [1,0]
          result2 <- ps
            .submitTransaction(
              submitterInfo(rt, alice, "X1"),
              transactionMeta(rt),
              emptyTransaction)
            .toScala
          result3 <- ps
            .submitTransaction(
              submitterInfo(rt, alice, "X2"),
              transactionMeta(rt),
              emptyTransaction)
            .toScala
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = Some(theOffset(1, 0)))
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(theOffset(2, 0))
          update.recordTime should be >= rt
          update should be(a[TransactionAccepted])
        }
      }

      "correctly implement transaction submission authorization" in participantState.use { ps =>
        val unallocatedParty = Ref.Party.assertFromString("nobody")
        for {
          lic <- ps.getLedgerInitialConditions().runWith(Sink.head)
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1,
              ),
            )
            .toScala

          // Submit without allocation
          _ <- ps
            .submitTransaction(
              submitterInfo(rt, unallocatedParty),
              transactionMeta(rt),
              emptyTransaction,
            )
            .toScala

          // Allocate a party and try the submission again with an allocated party.
          result <- ps
            .allocateParty(
              None /* no name hint, implementation decides party name */,
              Some("Somebody"),
              newSubmissionId(),
            )
            .toScala
          _ = result should be(a[SubmissionResult])

          //get the new party off state updates
          newParty <- ps
            .stateUpdates(beginAfter = Some(theOffset(1, 0)))
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
            .map(_._2.asInstanceOf[PartyAddedToParticipant].party)
          _ <- ps
            .submitTransaction(
              submitterInfo(rt, party = newParty),
              transactionMeta(rt),
              emptyTransaction,
            )
            .toScala

          Seq((offset1, update1), (offset2, update2), (offset3, update3), (offset4, update4)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(4)
            .runWith(Sink.seq)
          updates = Seq(update1, update2, update3, update4)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt

          offset1 should be(theOffset(0, 0))
          update1 should be(a[ConfigurationChanged])

          offset2 should be(theOffset(1, 0))
          inside(update2) {
            case CommandRejected(_, _, reason) =>
              reason should be(RejectionReason.PartyNotKnownOnLedger)
          }

          offset3 should be(theOffset(2, 0))
          update3 should be(a[PartyAddedToParticipant])

          offset4 should be(theOffset(3, 0))
          update4 should be(a[TransactionAccepted])
        }
      }
    }

    "submitConfiguration" should {
      "allow an administrator to submit a new configuration" in participantState.use { ps =>
        for {
          lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

          // Submit an initial configuration change
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1,
              ),
            )
            .toScala

          // Submit another configuration change that uses stale "current config".
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1,
                timeModel = TimeModel(
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                ).get,
              ),
            )
            .toScala

          Seq((_, update1), (_, update2)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(2)
            .runWith(Sink.seq)
        } yield {
          // The first submission should change the config.
          inside(update1) {
            case ConfigurationChanged(_, _, _, newConfiguration) =>
              newConfiguration should not be lic.config
          }

          // The second submission should get rejected.
          update2 should be(a[ConfigurationChangeRejected])
        }
      }

      "reject a duplicate submission" in participantState.use { ps =>
        val submissionIds = (newSubmissionId(), newSubmissionId())
        for {
          lic <- ps.getLedgerInitialConditions().runWith(Sink.head)

          // Submit an initial configuration change
          result1 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._1,
              config = lic.config.copy(
                generation = lic.config.generation + 1,
              ),
            )
            .toScala
          // this is a duplicate, which fails silently
          result2 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._1,
              config = lic.config.copy(
                generation = lic.config.generation + 2,
              ),
            )
            .toScala
          result3 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._2,
              config = lic.config.copy(
                generation = lic.config.generation + 2,
              ),
            )
            .toScala
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)

          // second submission is a duplicate, and is therefore dropped
          Seq(_, (offset2, update2)) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .take(2)
            .runWith(Sink.seq)
        } yield {
          offset2 should be(theOffset(2, 0))
          update2.recordTime should be >= rt
          inside(update2) {
            case ConfigurationChanged(_, submissionId, _, _) =>
              submissionId should be(submissionIds._2)
          }
        }
      }
    }

    "process commits serially" in participantState.use { ps =>
      val partyCount = 1000L
      val partyIds = 1L to partyCount
      val partyIdDigits = partyCount.toString.length
      val partyNames =
        partyIds
          .map(i => Ref.Party.assertFromString(s"party-%0${partyIdDigits}d".format(i)))
          .toVector

      val updatesF = ps
        .stateUpdates(beginAfter = None)
        .idleTimeout(IdleTimeout)
        .take(partyCount)
        .runWith(Sink.seq)
      for {
        results <- Future.sequence(
          partyNames.map(name =>
            ps.allocateParty(Some(name), Some(name), newSubmissionId()).toScala),
        )
        _ = all(results) should be(SubmissionResult.Acknowledged)
        updates <- updatesF
      } yield {
        val expectedOffsets = partyIds.map(i => theOffset(i - 1, 0)).toVector
        val actualOffsets = updates.map(_._1).sorted.toVector
        actualOffsets should be(expectedOffsets)

        val actualNames =
          updates.map(_._2.asInstanceOf[PartyAddedToParticipant].displayName).sorted.toVector
        actualNames should be(partyNames)
      }
    }

    if (isPersistent) {
      "store the ledger ID and re-use it" in {
        val ledgerId = newLedgerId()
        for {
          retrievedLedgerId1 <- newParticipantState(ledgerId).use { ps =>
            ps.getLedgerInitialConditions().map(_.ledgerId).runWith(Sink.head)
          }
          retrievedLedgerId2 <- newParticipantState().use { ps =>
            ps.getLedgerInitialConditions().map(_.ledgerId).runWith(Sink.head)
          }
        } yield {
          retrievedLedgerId1 should be(ledgerId)
          retrievedLedgerId2 should be(ledgerId)
        }
      }

      "reject a different ledger ID" in {
        val ledgerId = newLedgerId()
        val attemptedLedgerId = newLedgerId()
        for {
          _ <- newParticipantState(ledgerId).use { _ =>
            Future.unit
          }
          exception <- newParticipantState(attemptedLedgerId).use { _ =>
            Future.unit
          }.failed
        } yield {
          exception should be(a[LedgerIdMismatchException])
          val mismatchException = exception.asInstanceOf[LedgerIdMismatchException]
          mismatchException.existingLedgerId should be(ledgerId)
          mismatchException.providedLedgerId should be(attemptedLedgerId)
        }
      }

      "resume where it left off on restart" in {
        val ledgerId = newLedgerId()
        for {
          _ <- newParticipantState(ledgerId).use { ps =>
            for {
              _ <- ps
                .allocateParty(None, Some("party-1"), newSubmissionId())
                .toScala
            } yield ()
          }
          updates <- newParticipantState().use { ps =>
            for {
              _ <- ps
                .allocateParty(None, Some("party-2"), newSubmissionId())
                .toScala
              updates <- ps
                .stateUpdates(beginAfter = None)
                .idleTimeout(IdleTimeout)
                .take(2)
                .runWith(Sink.seq)
            } yield updates.map(_._2)
          }
        } yield {
          all(updates) should be(a[PartyAddedToParticipant])
          val displayNames = updates.map(_.asInstanceOf[PartyAddedToParticipant].displayName)
          displayNames should be(Seq("party-1", "party-2"))
        }
      }
    }
  }

  private def submitterInfo(rt: Timestamp, party: Ref.Party, commandId: String = "X") =
    SubmitterInfo(
      submitter = party,
      applicationId = Ref.LedgerString.assertFromString("tests"),
      commandId = Ref.LedgerString.assertFromString(commandId),
      maxRecordTime = inTheFuture(10.seconds),
    )

  private def theOffset(first: Long, rest: Long*): Offset =
    Offset(Array(first + startIndex, rest: _*))

  private def inTheFuture(duration: FiniteDuration): Timestamp =
    rt.add(Duration.ofNanos(duration.toNanos))
}

object ParticipantStateIntegrationSpecBase {
  type ParticipantState = ReadService with WriteService

  private val IdleTimeout = 5.seconds
  private val emptyTransaction: Transaction.AbsTransaction =
    GenTransaction(HashMap.empty, ImmArray.empty, Some(InsertOrdSet.empty))

  private val participantId: ParticipantId = Ref.ParticipantId.assertFromString("test-participant")
  private val sourceDescription = Some("provided by test")

  private val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }
  private val archives =
    darReader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar"))).get.all

  private val alice = Ref.Party.assertFromString("alice")

  private def newLedgerId(): LedgerId =
    Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")

  private def newSubmissionId(): SubmissionId =
    Ref.LedgerString.assertFromString(s"submission-${UUID.randomUUID()}")

  private def transactionMeta(let: Timestamp) =
    TransactionMeta(
      ledgerEffectiveTime = let,
      workflowId = Some(Ref.LedgerString.assertFromString("tests")),
      submissionSeed = Some(
        crypto.Hash.assertFromString(
          "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
    )

  private def matchPackageUpload(
      update: Update,
      expectedSubmissionId: SubmissionId,
      expectedArchives: List[DamlLf.Archive],
  ): Assertion =
    inside(update) {
      case PublicPackageUpload(
          actualArchives,
          actualSourceDescription,
          _,
          Some(actualSubmissionId),
          ) =>
        actualArchives.map(_.getHash).toSet should be(expectedArchives.map(_.getHash).toSet)
        actualSourceDescription should be(sourceDescription)
        actualSubmissionId should be(expectedSubmissionId)
    }

  private def matchTransaction(update: Update, expectedCommandId: String): Assertion =
    inside(update) {
      case TransactionAccepted(Some(SubmitterInfo(_, _, actualCommandId, _)), _, _, _, _, _) =>
        actualCommandId should be(expectedCommandId)
    }
}
