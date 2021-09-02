// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Clock, Duration}
import java.util.UUID

import akka.Done
import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerId, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.OffsetBuilder.{fromLong => toOffset}
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase._
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.Update._
import com.daml.ledger.participant.state.v2._
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.Decode
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party.ordering
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.MismatchException
import com.daml.platform.testing.TestDarReader
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.google.rpc.code.Code
import org.scalatest.Inside._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.collection.compat._
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

//noinspection DuplicatedCode
abstract class ParticipantStateIntegrationSpecBase(implementationName: String)(implicit
    testExecutionContext: ExecutionContext = ExecutionContext.global
) extends AsyncWordSpec
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  private implicit val resourceContext: ResourceContext = ResourceContext(testExecutionContext)
  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  // Can be used by [[participantStateFactory]] to get a stable ID throughout the test.
  // For example, for initializing a database.
  private var testId: String = _

  private var rt: Timestamp = _

  // This can be overridden by tests for ledgers that don't start at 0.
  protected val startIndex: Long = 0

  // This can be overridden by tests for in-memory or otherwise ephemeral ledgers.
  protected val isPersistent: Boolean = true

  protected def participantStateFactory(
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      testId: String,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantState]

  private def participantState: ResourceOwner[ParticipantState] =
    newParticipantState(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))

  private def newParticipantState(
      ledgerId: LedgerId
  ): ResourceOwner[ParticipantState] =
    newLoggingContext { implicit loggingContext =>
      participantStateFactory(ledgerId, participantId, testId, new Metrics(new MetricRegistry))
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
      newParticipantState(ledgerId = ledgerId).use { ps =>
        for {
          conditions <- ps
            .ledgerInitialConditions()
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
          result <- ps.uploadPackages(submissionId, List(anArchive), sourceDescription).toScala
          _ = result should be(SubmissionResult.Acknowledged)
          (offset, update) <- ps
            .stateUpdates(beginAfter = None)
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
        } yield {
          offset should be(toOffset(1))
          update.recordTime should be >= rt
          matchPackageUpload(update, submissionId, List(anArchive))
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
          offset should be(toOffset(1))
          update.recordTime should be >= rt
          matchPackageUpload(update, submissionId, archives)
        }
      }

      "remove a duplicate package from the update" in participantState.use { ps =>
        val (subId1, subId2, subId3) =
          (newSubmissionId(), newSubmissionId(), newSubmissionId())

        for {
          result1 <- ps.uploadPackages(subId1, List(anArchive), sourceDescription).toScala
          (offset1, update1) <- waitForNextUpdate(ps, None)
          result2 <- ps.uploadPackages(subId2, List(anArchive), sourceDescription).toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))
          result3 <- ps.uploadPackages(subId3, List(anotherArchive), sourceDescription).toScala
          (offset3, update3) <- waitForNextUpdate(ps, Some(offset2))
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          updates = Seq(update1, update2, update3)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt
          // first upload arrives as head update:
          offset1 should be(toOffset(1))
          matchPackageUpload(update1, subId1, List(anArchive))
          offset2 should be(toOffset(2))
          matchPackageUpload(update2, subId2, List())
          offset3 should be(toOffset(3))
          matchPackageUpload(update3, subId3, List(anotherArchive))
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
          offset should be(toOffset(1))
          update.recordTime should be >= rt
          inside(update) { case PublicPackageUploadRejected(actualSubmissionId, _, _) =>
            actualSubmissionId should be(submissionId)
          }
        }
      }

      "reject a duplicate submission" in participantState.use { ps =>
        val submissionIds = (newSubmissionId(), newSubmissionId())

        for {
          result1 <- ps.uploadPackages(submissionIds._1, List(anArchive), sourceDescription).toScala
          (offset1, _) <- waitForNextUpdate(ps, None)
          // Second submission is a duplicate, it fails without an update.
          result2 <- ps.uploadPackages(submissionIds._1, List(anArchive), sourceDescription).toScala
          result3 <- ps
            .uploadPackages(submissionIds._2, List(anotherArchive), sourceDescription)
            .toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
        } yield {
          offset2 should be(toOffset(3))
          update2.recordTime should be >= rt
          inside(update2) { case PublicPackageUpload(_, _, _, Some(submissionId)) =>
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
          (offset, update) <- waitForNextUpdate(ps, None)
        } yield {
          offset should be(toOffset(1))
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
          (offset, update) <- waitForNextUpdate(ps, None)
        } yield {
          offset should be(toOffset(1))
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
          (offset1, _) <- waitForNextUpdate(ps, None)
          // Second submission is a duplicate, it should not generate an update.
          result2 <- ps.allocateParty(hints._2, Some(displayNames._2), submissionIds._1).toScala
          result3 <- ps.allocateParty(hints._2, Some(displayNames._2), submissionIds._2).toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
        } yield {
          offset2 should be(toOffset(3))
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
          (offset1, _) <- waitForNextUpdate(ps, None)
          result2 <- ps.allocateParty(hint, displayName, newSubmissionId()).toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))
          results = Seq(result1, result2)
          _ = all(results) should be(SubmissionResult.Acknowledged)
        } yield {
          offset2 should be(toOffset(2))
          update2.recordTime should be >= rt
          inside(update2) { case PartyAllocationRejected(_, _, _, rejectionReason) =>
            rejectionReason should be("Party already exists")
          }
        }
      }
    }

    "submitTransaction" should {
      "provide an update after a transaction submission" in participantState.use { ps =>
        for {
          _ <- ps.allocateParty(hint = Some(alice), None, newSubmissionId()).toScala
          (offset1, _) <- waitForNextUpdate(ps, None)
          _ <- ps
            .submitTransaction(
              submitterInfo(alice),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset2, _) <- waitForNextUpdate(ps, Some(offset1))
        } yield {
          offset2 should be(toOffset(2))
        }
      }

      "reject duplicate commands" ignore participantState.use { ps =>
        val firstCommandId = "X1"
        val secondCommandId = "X2"
        for {
          result1 <- ps.allocateParty(hint = Some(alice), None, newSubmissionId()).toScala
          (offset1, update1) <- waitForNextUpdate(ps, None)
          result2 <- ps
            .submitTransaction(
              submitterInfo(alice, firstCommandId),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))
          // Below submission is a duplicate, should get dropped.
          result3 <- ps
            .submitTransaction(
              submitterInfo(alice, firstCommandId),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          result4 <- ps
            .submitTransaction(
              submitterInfo(alice, secondCommandId),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset3, update3) <- waitForNextUpdate(ps, Some(offset2))
          results = Seq(result1, result2, result3, result4)
          _ = all(results) should be(SubmissionResult.Acknowledged)
          updates = Seq(update1, update2, update3)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt

          offset1 should be(toOffset(1))
          update1 should be(a[PartyAddedToParticipant])

          offset2 should be(toOffset(2))
          matchTransaction(update2, firstCommandId)

          offset3 should be(toOffset(4))
          matchTransaction(update3, secondCommandId)
        }
      }

      "return the third update with beginAfter=2" in participantState.use { ps =>
        for {
          result1 <- ps
            .allocateParty(hint = Some(alice), None, newSubmissionId())
            .toScala // offset now at [1,0]
          (offset1, _) <- waitForNextUpdate(ps, None)
          result2 <- ps
            .submitTransaction(
              submitterInfo(alice, "X1"),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset2, _) <- waitForNextUpdate(ps, Some(offset1))
          result3 <- ps
            .submitTransaction(
              submitterInfo(alice, "X2"),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset3, update3) <- waitForNextUpdate(ps, Some(offset2))
          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
        } yield {
          offset3 should be(toOffset(3))
          update3.recordTime should be >= rt
          update3 should be(a[TransactionAccepted])
        }
      }

      "correctly implement transaction submission authorization" in participantState.use { ps =>
        val unallocatedParty = Ref.Party.assertFromString("nobody")
        for {
          lic <- ps.ledgerInitialConditions().runWith(Sink.head)
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1
              ),
            )
            .toScala
          (offset1, update1) <- waitForNextUpdate(ps, None)

          // Submit without allocation
          _ <- ps
            .submitTransaction(
              submitterInfo(unallocatedParty),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))

          // Allocate a party and try the submission again with an allocated party.
          result <- ps
            .allocateParty(
              None /* no name hint, implementation decides party name */,
              Some("Somebody"),
              newSubmissionId(),
            )
            .toScala
          _ = result should be(a[SubmissionResult])
          (offset3, update3) <- waitForNextUpdate(ps, Some(offset2))

          //get the new party off state updates
          newParty <- ps
            .stateUpdates(beginAfter = Some(toOffset(2)))
            .idleTimeout(IdleTimeout)
            .runWith(Sink.head)
            .map(_._2.asInstanceOf[PartyAddedToParticipant].party)
          _ <- ps
            .submitTransaction(
              submitterInfo(party = newParty),
              transactionMeta(rt),
              TransactionBuilder.EmptySubmitted,
              DefaultInterpretationCost,
            )
            .toScala
          (offset4, update4) <- waitForNextUpdate(ps, Some(offset3))

          updates = Seq(update1, update2, update3, update4)
        } yield {
          all(updates.map(_.recordTime)) should be >= rt

          offset1 should be(toOffset(1))
          update1 should be(a[ConfigurationChanged])

          offset2 should be(toOffset(2))
          inside(update2) { case CommandRejected(_, _, FinalReason(status)) =>
            status.code should be(Code.INVALID_ARGUMENT.value)
          }

          offset3 should be(toOffset(3))
          update3 should be(a[PartyAddedToParticipant])

          offset4 should be(toOffset(4))
          update4 should be(a[TransactionAccepted])
        }
      }
    }

    "submitConfiguration" should {
      "allow an administrator to submit a new configuration" in participantState.use { ps =>
        for {
          lic <- ps.ledgerInitialConditions().runWith(Sink.head)

          // Submit an initial configuration change
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1
              ),
            )
            .toScala
          (offset1, update1) <- waitForNextUpdate(ps, None)

          // Submit another configuration change that uses stale "current config".
          _ <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = newSubmissionId(),
              config = lic.config.copy(
                generation = lic.config.generation + 1,
                timeModel = LedgerTimeModel(
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                  Duration.ofSeconds(123),
                ).get,
              ),
            )
            .toScala
          (_, update2) <- waitForNextUpdate(ps, Some(offset1))
        } yield {
          // The first submission should change the config.
          inside(update1) { case ConfigurationChanged(_, _, _, newConfiguration) =>
            newConfiguration should not be lic.config
          }

          // The second submission should get rejected.
          update2 should be(a[ConfigurationChangeRejected])
        }
      }

      "reject a duplicate submission" in participantState.use { ps =>
        val submissionIds = (newSubmissionId(), newSubmissionId())
        for {
          lic <- ps.ledgerInitialConditions().runWith(Sink.head)

          // Submit an initial configuration change
          result1 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._1,
              config = lic.config.copy(
                generation = lic.config.generation + 1
              ),
            )
            .toScala
          (offset1, _) <- waitForNextUpdate(ps, None)
          // this is a duplicate, which fails silently
          result2 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._1,
              config = lic.config.copy(
                generation = lic.config.generation + 2
              ),
            )
            .toScala
          result3 <- ps
            .submitConfiguration(
              maxRecordTime = inTheFuture(10.seconds),
              submissionId = submissionIds._2,
              config = lic.config.copy(
                generation = lic.config.generation + 2
              ),
            )
            .toScala
          (offset2, update2) <- waitForNextUpdate(ps, Some(offset1))

          results = Seq(result1, result2, result3)
          _ = all(results) should be(SubmissionResult.Acknowledged)
        } yield {
          offset2 should be(toOffset(3))
          update2.recordTime should be >= rt
          inside(update2) { case ConfigurationChanged(_, submissionId, _, _) =>
            submissionId should be(submissionIds._2)
          }
        }
      }
    }

    "process many party allocations" in participantState.use { ps =>
      val partyCount = 1000L
      val partyIds = 1L to partyCount
      val partyIdDigits = partyCount.toString.length
      val partyNames =
        partyIds
          .map(i => Ref.Party.assertFromString(s"party-%0${partyIdDigits}d".format(i)))
          .to(SortedSet)

      val expectedOffsets = partyIds.map(i => toOffset(i)).to(SortedSet)

      val updates = mutable.Buffer.empty[(Offset, Update)]
      val stateUpdatesF = ps
        .stateUpdates(beginAfter = None)
        .idleTimeout(IdleTimeout)
        .take(partyCount)
        .runWith(Sink.foreach { update =>
          updates.synchronized {
            updates += update
            ()
          }
        })
      for {
        results <- Future.traverse(partyNames.toVector)(name =>
          ps.allocateParty(Some(name), Some(name), newSubmissionId()).toScala
        )
        _ = all(results) should be(SubmissionResult.Acknowledged)

        _ <- stateUpdatesF.transform {
          case Success(Done) => Success(())
          case Failure(exception: TimeoutException) =>
            val acceptedPartyNames =
              updates.map(_._2.asInstanceOf[PartyAddedToParticipant].displayName)
            val missingPartyNames = partyNames.map(name => name: String) -- acceptedPartyNames
            Failure(
              new RuntimeException(
                s"Timed out with parties missing: ${missingPartyNames.mkString(", ")}",
                exception,
              )
            )
          case Failure(exception) => Failure(exception)
        }
      } yield {
        updates.size should be(partyCount)

        val (actualOffsets, actualUpdates) = updates.unzip
        all(actualUpdates) should be(a[PartyAddedToParticipant])
        actualOffsets.to(SortedSet) should be(expectedOffsets)

        val actualNames =
          actualUpdates.map(_.asInstanceOf[PartyAddedToParticipant].displayName)
        actualNames.to(SortedSet) should be(partyNames)
      }
    }

    if (isPersistent) {
      "store the ledger ID and re-use it" in {
        val ledgerId = newLedgerId()
        for {
          retrievedLedgerId1 <- newParticipantState(ledgerId = ledgerId).use { ps =>
            ps.ledgerInitialConditions().map(_.ledgerId).runWith(Sink.head)
          }
          retrievedLedgerId2 <- newParticipantState(ledgerId = ledgerId).use { ps =>
            ps.ledgerInitialConditions().map(_.ledgerId).runWith(Sink.head)
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
          _ <- newParticipantState(ledgerId = ledgerId).use { _ =>
            Future.unit
          }
          exception <- newParticipantState(ledgerId = attemptedLedgerId).use { _ =>
            Future.unit
          }.failed
        } yield {
          exception should be(a[MismatchException.LedgerId])
          val mismatchException = exception.asInstanceOf[MismatchException.LedgerId]
          mismatchException.existing should be(ledgerId)
          mismatchException.provided should be(attemptedLedgerId)
        }
      }

      "resume where it left off on restart" in {
        val ledgerId = newLedgerId()
        for {
          _ <- newParticipantState(ledgerId = ledgerId).use { ps =>
            for {
              _ <- ps
                .allocateParty(None, Some("party-1"), newSubmissionId())
                .toScala
            } yield ()
          }
          updates <- newParticipantState(ledgerId = ledgerId).use { ps =>
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

  private def submitterInfo(party: Ref.Party, commandId: String = "X") =
    SubmitterInfo(
      actAs = List(party),
      applicationId = Ref.LedgerString.assertFromString("tests"),
      commandId = Ref.LedgerString.assertFromString(commandId),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(10)),
      submissionId = Ref.LedgerString.assertFromString("submissionId"),
      ledgerConfiguration =
        Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(1)),
    )

  private def inTheFuture(duration: FiniteDuration): Timestamp =
    rt.add(Duration.ofNanos(duration.toNanos))

  private def waitForNextUpdate(
      ps: ParticipantState,
      offset: Option[Offset],
  ): Future[(Offset, Update)] =
    ps.stateUpdates(beginAfter = offset)
      .idleTimeout(IdleTimeout)
      .runWith(Sink.head)
}

object ParticipantStateIntegrationSpecBase {
  type ParticipantState = ReadService with WriteService

  private val IdleTimeout: FiniteDuration = 15.seconds
  private val DefaultInterpretationCost = 0L

  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("test-participant")
  private val sourceDescription = Some("provided by test")

  private val archives = TestDarReader.readCommonTestDar(ModelTestDar).get.all

  // 2 self consistent archives
  protected val List(anArchive, anotherArchive) =
    archives
      .sortBy(_.getSerializedSize) // look at the smallest archives first to limit decoding work
      .iterator
      .filter(Decode.assertDecodeArchive(_)._2.directDeps.isEmpty)
      .take(2)
      .toList

  private val alice = Ref.Party.assertFromString("alice")

  private def newLedgerId(): LedgerId =
    Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")

  private def newSubmissionId(): Ref.SubmissionId =
    Ref.LedgerString.assertFromString(s"submission-${UUID.randomUUID()}")

  private def transactionMeta(let: Timestamp) =
    TransactionMeta(
      ledgerEffectiveTime = let,
      workflowId = Some(Ref.LedgerString.assertFromString("tests")),
      submissionTime = let.addMicros(-1000),
      submissionSeed = crypto.Hash.assertFromString(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      ),
      optUsedPackages = Some(Set.empty),
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

  private def matchPackageUpload(
      update: Update,
      expectedSubmissionId: Ref.SubmissionId,
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
      case TransactionAccepted(
            Some(CompletionInfo(_, _, actualCommandId, _, _)),
            _,
            _,
            _,
            _,
            _,
            _,
          ) =>
        actualCommandId should be(expectedCommandId)
    }
}
