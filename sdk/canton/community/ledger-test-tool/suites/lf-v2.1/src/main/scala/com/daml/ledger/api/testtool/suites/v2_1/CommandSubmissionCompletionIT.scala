// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerTestSuite,
  TestConstraints,
  TimeoutException,
  WithTimeout,
}
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, RequestValidationErrors}
import org.scalatest.Inside.inside

import java.util.regex.Pattern
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class CommandSubmissionCompletionIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "CSCCompletions",
    "Read completions correctly with a correct user identifier and reading party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      completions <- ledger.firstCompletions(party)
    } yield {
      val commandId =
        assertSingleton("Expected only one completion", completions.map(_.commandId))
      assert(
        commandId == request.commands.get.commandId,
        "Wrong command identifier on completion",
      )
    }
  })

  test(
    "CSCParticipantBeginCompletions",
    "Read completions from participant begin",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      completionRequest = ledger
        .completionStreamRequest(0L)(party)
      completions <- ledger.firstCompletions(completionRequest)
    } yield {
      assert(completions.nonEmpty, "Completions should not have been empty")
    }
  })

  test(
    "CSCNoCompletionsWithoutRightUserId",
    "Read no completions without the correct user identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      invalidRequest = ledger
        .completionStreamRequest()(party)
        .update(_.userId := "invalid-user-id")
      failure <- WithTimeout(5.seconds)(ledger.firstCompletions(invalidRequest))
        .mustFail("subscribing to completions with an invalid user ID")
    } yield {
      assert(failure == TimeoutException, "Timeout expected")
    }
  })

  test(
    "CSCAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing to completions past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      invalidRequest = ledger
        .completionStreamRequest()(party)
        .update(_.beginExclusive := futureOffset)
      failure <- ledger
        .firstCompletions(invalidRequest)
        .mustFail("subscribing to completions past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "CSCNoCompletionsWithoutRightParty",
    "Read no completions without the correct party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, notTheSubmittingParty))) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      failure <- WithTimeout(5.seconds)(ledger.firstCompletions(notTheSubmittingParty))
        .mustFail("subscribing to completions with the wrong party")
    } yield {
      assert(failure == TimeoutException, "Timeout expected")
    }
  })

  test(
    "CSCRefuseBadChoice",
    "The submission of an exercise of a choice that does not exist should yield INVALID_ARGUMENT",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly(
      "Problem creating faulty JSON from a faulty GRPC call"
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val badChoice = "THIS_IS_NOT_A_VALID_CHOICE"
    for {
      dummy <- ledger.create(party, new Dummy(party))
      exercise = dummy.exerciseDummyChoice1().commands
      wrongExercise = updateCommands(exercise, _.update(_.exercise.choice := badChoice))
      wrongRequest = ledger.submitRequest(party, wrongExercise)
      failure <- ledger.submit(wrongRequest).mustFail("submitting an invalid choice")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        Some(
          Pattern.compile(
            "(unknown|Couldn't find requested) choice " + badChoice
          )
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCDisallowEmptyTransactionsSubmission",
    "The submission of an empty command should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val emptyRequest = ledger.submitRequest(party)
    for {
      failure <- ledger.submit(emptyRequest).mustFail("submitting an empty command")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.MissingField,
        Some("commands"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCHandleMultiPartySubscriptions",
    "Listening for completions should support multi-party subscriptions",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val aliceRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger.submitRequest(bob, new Dummy(bob).create.commands)
    val aliceBobRequest = {
      val originalCommands = ledger.submitRequest(alice, new Dummy(alice).create.commands)
      originalCommands.withCommands(
        originalCommands.getCommands.copy(
          actAs = Seq(alice, bob)
        )
      )
    }

    val aliceCommandId = aliceRequest.getCommands.commandId
    val bobCommandId = bobRequest.getCommands.commandId
    val aliceBobCommandId = aliceBobRequest.getCommands.commandId

    for {
      _ <- ledger.submit(aliceRequest)
      _ <- ledger.submit(bobRequest)
      _ <- ledger.submit(aliceBobRequest)
      _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == aliceCommandId))
      _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == bobCommandId))
      aliceBobCompletionForAliceBob <- WithTimeout(5.seconds)(
        ledger.findCompletion(alice, bob)(_.commandId == aliceBobCommandId)
      )
      // as all the commands are already visible on the ledger we can go to lower timeouts
      _ <- WithTimeout(2.seconds)(ledger.findCompletion(alice)(_.commandId == aliceCommandId))
      _ <- WithTimeout(2.seconds)(ledger.findCompletion(alice)(_.commandId == bobCommandId))
        .mustFailWith("alice should not be able to look up bob's command")(_ == TimeoutException)
      aliceBobCompletionForAlice <- WithTimeout(2.seconds)(
        ledger.findCompletion(alice)(_.commandId == aliceBobCommandId)
      )
      _ <- WithTimeout(2.seconds)(ledger.findCompletion(bob)(_.commandId == aliceCommandId))
        .mustFailWith("bob should not be able to look up alice's command")(_ == TimeoutException)
      _ <- WithTimeout(2.seconds)(ledger.findCompletion(bob)(_.commandId == bobCommandId))
      aliceBobCompletionForBob <- WithTimeout(2.seconds)(
        ledger.findCompletion(bob)(_.commandId == aliceBobCommandId)
      )
    } yield {
      assertEquals(
        "If filtered with both alice and bob, the multi-party submissions should have both act_as parties.",
        aliceBobCompletionForAliceBob.map(_.actAs.toSet),
        Some(Set(alice.underlying.getValue, bob.underlying.getValue)),
      )
      assertEquals(
        "If filtered with only alice, the multi-party submissions should have only alice act_as party.",
        aliceBobCompletionForAlice.map(_.actAs.toSet),
        Some(Set(alice.underlying.getValue)),
      )
      assertEquals(
        "If filtered with only bob, the multi-party submissions should have only bob act_as party.",
        aliceBobCompletionForBob.map(_.actAs.toSet),
        Some(Set(bob.underlying.getValue)),
      )
    }
  })

  test(
    "CSCSubmitInvalidSynchronizerId",
    "Submit should fail for invalid synchronizer ids",
    allocate(SingleParty),
  ) { implicit ec =>
    { case Participants(Participant(ledger, Seq(party, _*))) =>
      val invalidSynchronizerId = "invalidSynchronizerId"
      val request = ledger
        .submitRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := invalidSynchronizerId)
      for {
        failure <- ledger
          .submit(request)
          .mustFail(
            "submitting a request with an invalid synchronizer id"
          )
      } yield assertGrpcError(
        failure,
        RequestValidationErrors.InvalidField,
        Some(
          s"Invalid field synchronizer_id: Invalid unique identifier `$invalidSynchronizerId` with missing namespace."
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  }

  test(
    "CSCSubmitUnknownSynchronizerId",
    "Submit should fail for unknown synchronizer ids",
    allocate(SingleParty) expectingMinimumNumberOfSynchronizers (1),
  ) { implicit ec =>
    { case Participants(Participant(ledger, Seq(party, _*))) =>
      val synchronizer =
        inside(party.initialSynchronizers) { case Seq(synchronizer, _*) =>
          synchronizer
        }
      val unknownSynchronizerId = s"unknown_$synchronizer"
      val request = ledger
        .submitRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := unknownSynchronizerId)
      for {
        failure <- ledger
          .submit(request)
          .mustFail(
            "submitting a request with an unknown synchronizer id"
          )
      } yield assertGrpcError(
        failure,
        InvalidPrescribedSynchronizerId,
        Some(
          s"Cannot submit transaction to prescribed synchronizer"
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  }

  test(
    "CSCSubmitWithPrescribedSynchronizerId",
    "Submit should use the prescribed synchronizer id when routing the submission",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
  ) { implicit ec =>
    { case Participants(Participant(ledger, Seq(party, _*))) =>
      def assertSubmitRoutedToSynchronizer(targetSynchronizer: String): Future[Unit] = for {
        _ <- Future.unit
        requestForSynchronizer1 = ledger
          .submitRequest(party, new Dummy(party).create.commands)
          .update(_.commands.synchronizerId := targetSynchronizer)
        _ <- ledger.submit(requestForSynchronizer1)
        firstSynchronizerCompletion <- WithTimeout(5.seconds)(
          ledger.findCompletion(party)(_.commandId == requestForSynchronizer1.getCommands.commandId)
        ).map(
          _.getOrElse(
            fail(s"completion not found for ${requestForSynchronizer1.getCommands.commandId}")
          )
        )
        actualSynchronizerId <- ledger
          .transactionById(firstSynchronizerCompletion.updateId, Seq(party), AcsDelta)
          .map(_.synchronizerId)
      } yield assertEquals(actualSynchronizerId, targetSynchronizer)

      val (synchronizer1, synchronizer2) =
        inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
          synchronizer1 -> synchronizer2
        }

      for {
        _ <- assertSubmitRoutedToSynchronizer(synchronizer1)
        _ <- assertSubmitRoutedToSynchronizer(synchronizer2)
      } yield ()
    }
  }
}
