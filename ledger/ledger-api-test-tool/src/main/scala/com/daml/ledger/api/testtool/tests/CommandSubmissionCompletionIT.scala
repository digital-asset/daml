// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.model.Test.Dummy
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.platform.testing.{TimeoutException, WithTimeout}
import io.grpc.Status

import scala.concurrent.duration.DurationInt

final class CommandSubmissionCompletionIT extends LedgerTestSuite {

  test(
    "CSCCompletions",
    "Read completions correctly with a correct application identifier and reading party",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val request = ledger.submitRequest(party, Dummy(party).create.command)
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
    "CSCNoCompletionsWithoutRightAppId",
    "Read no completions without the correct application identifier",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val request = ledger.submitRequest(party, Dummy(party).create.command)
      for {
        _ <- ledger.submit(request)
        invalidRequest = ledger
          .completionStreamRequest()(party)
          .update(_.applicationId := "invalid-application-id")
        failed <- WithTimeout(5.seconds)(ledger.firstCompletions(invalidRequest)).failed
      } yield {
        assert(failed == TimeoutException, "Timeout expected")
      }
  })

  test(
    "CSCAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing to completions past the ledger end",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val request = ledger.submitRequest(party, Dummy(party).create.command)
      for {
        _ <- ledger.submit(request)
        futureOffset <- ledger.offsetBeyondLedgerEnd()
        invalidRequest = ledger
          .completionStreamRequest()(party)
          .update(_.offset := futureOffset)
        failure <- ledger.firstCompletions(invalidRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.OUT_OF_RANGE, "is after ledger end")
      }
  })

  test(
    "CSCNoCompletionsWithoutRightParty",
    "Read no completions without the correct party",
    allocate(TwoParties),
  )(implicit ec => {
    case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
      val request = ledger.submitRequest(party, Dummy(party).create.command)
      for {
        _ <- ledger.submit(request)
        failed <- WithTimeout(5.seconds)(ledger.firstCompletions(notTheSubmittingParty)).failed
      } yield {
        assert(failed == TimeoutException, "Timeout expected")
      }
  })

  test(
    "CSCRefuseBadChoice",
    "The submission of an exercise of a choice that does not exist should yield INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val badChoice = "THIS_IS_NOT_A_VALID_CHOICE"
      for {
        dummy <- ledger.create(party, Dummy(party))
        exercise = dummy.exerciseDummyChoice1(party).command
        wrongExercise = exercise.update(_.exercise.choice := badChoice)
        wrongRequest = ledger.submitRequest(party, wrongExercise)
        failure <- ledger.submit(wrongRequest).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          s"Couldn't find requested choice $badChoice",
        )
      }
  })

  test(
    "CSCSubmitWithInvalidLedgerId",
    "Submit should fail for an invalid ledger identifier",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val invalidLedgerId = "CSsubmitAndWaitInvalidLedgerId"
      val request = ledger
        .submitRequest(party, Dummy(party).create.command)
        .update(_.commands.ledgerId := invalidLedgerId)
      for {
        failure <- ledger.submit(request).failed
      } yield
        assertGrpcError(
          failure,
          Status.Code.NOT_FOUND,
          s"Ledger ID '$invalidLedgerId' not found.",
        )
  })

  test(
    "CSCDisallowEmptyTransactionsSubmission",
    "The submission of an empty command should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val emptyRequest = ledger.submitRequest(party)
      for {
        failure <- ledger.submit(emptyRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: commands")
      }
  })

  test(
    "CSCHandleMultiPartySubscriptions",
    "Listening for completions should support multi-party subscriptions",
    allocate(TwoParties),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob)) =>
      val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
      val bobRequest = ledger.submitRequest(bob, Dummy(bob).create.command)
      val aliceCommandId = aliceRequest.getCommands.commandId
      val bobCommandId = bobRequest.getCommands.commandId
      for {
        _ <- ledger.submit(aliceRequest)
        _ <- ledger.submit(bobRequest)
        _ <- WithTimeout(5.seconds)(
          ledger.findCompletion(alice, bob)(_.commandId == aliceCommandId))
        _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == bobCommandId))
      } yield {
        // Nothing to do, if the two completions are found the test is passed
      }
  })

}
