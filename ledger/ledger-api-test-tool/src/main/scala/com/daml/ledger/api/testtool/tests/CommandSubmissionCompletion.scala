// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.platform.testing.{TimeoutException, WithTimeout}
import io.grpc.Status
import scalaz.Tag

import scala.concurrent.duration.DurationInt

final class CommandSubmissionCompletion(session: LedgerSession) extends LedgerTestSuite(session) {

  test(
    "CSCCompletions",
    "Read completions correctly with a correct application identifier and reading party",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(request)
        completions <- ledger.firstCompletions(party)
      } yield {
        val commandId =
          assertSingleton("Expected only one completion", completions.map(_.commandId))
        assert(
          commandId == request.commands.get.commandId,
          "Wrong command identifier on completion")
      }
  }

  test(
    "CSCNoCompletionsWithoutRightAppId",
    "Read no completions without the correct application identifier",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(request)
        invalidRequest = ledger
          .completionStreamRequest(party)
          .update(_.applicationId := "invalid-application-id")
        failed <- WithTimeout(5.seconds)(ledger.firstCompletions(invalidRequest)).failed
      } yield {
        assert(failed == TimeoutException, "Timeout expected")
      }
  }

  test(
    "CSCNoCompletionsWithoutRightParty",
    "Read no completions without the correct party",
    allocate(TwoParties)) {
    case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(request)
        failed <- WithTimeout(5.seconds)(ledger.firstCompletions(notTheSubmittingParty)).failed
      } yield {
        assert(failed == TimeoutException, "Timeout expected")
      }
  }

  test(
    "CSCRefuseBadChoice",
    "The submission of an exercise of a choice that does not exist should yield INVALID_ARGUMENT",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      val badChoice = "THIS_IS_NOT_A_VALID_CHOICE"
      for {
        dummy <- ledger.create(party, Dummy(party))
        exercise = dummy.exerciseDummyChoice1(party).command
        wrongExercise = exercise.update(_.exercise.choice := badChoice)
        wrongRequest <- ledger.submitRequest(party, wrongExercise)
        failure <- ledger.submit(wrongRequest).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          s"Couldn't find requested choice $badChoice")
      }
  }

  test(
    "CSCSubmitWithInvalidLedgerId",
    "Submit should fail for an invalid ledger identifier",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      val invalidLedgerId = "CSsubmitAndWaitInvalidLedgerId"
      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
        failure <- ledger.submit(badLedgerId).failed
      } yield
        assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
  }

  test(
    "CSCDisallowEmptyTransactionsSubmission",
    "The submission of an empty command should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        emptyRequest <- ledger.submitRequest(party)
        failure <- ledger.submit(emptyRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: commands")
      }
  }

  test(
    "CSCHandleMultiPartySubscriptions",
    "Listening for completions should support multi-party subscriptions",
    allocate(TwoParties)) {
    case Participants(Participant(ledger, alice, bob)) =>
      val a = UUID.randomUUID.toString
      val b = UUID.randomUUID.toString
      for {
        aliceRequest <- ledger.submitRequest(alice, Dummy(alice).create.command)
        bobRequest <- ledger.submitRequest(bob, Dummy(bob).create.command)
        _ <- ledger.submit(aliceRequest.update(_.commands.commandId := a))
        _ <- ledger.submit(bobRequest.update(_.commands.commandId := b))
        _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == a))
        _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == b))
      } yield {
        // Nothing to do, if the two completions are found the test is passed
      }
  }

  test(
    "CSCEmitPeriodicCheckpoints",
    "The CommandCompletionService should emit periodic checkpoints (at least 2 over 10 seconds)",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      WithTimeout(10.seconds)(ledger.checkpoints(2, party)).map(_ => ())
  }

  test(
    "CSCEmitExclusiveEndpoints",
    "Checkpoint offsets should not overlap with completion offsets",
    allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        first <- ledger.create(party, Dummy(party)).map(Tag.unwrap)
        cp1 <- ledger.nextCheckpoint(party).map(_.getOffset)
        second <- ledger.create(party, Dummy(party)).map(Tag.unwrap)
        cp2 <- ledger.nextCheckpoint(party).map(_.getOffset)
        third <- ledger.create(party, Dummy(party)).map(Tag.unwrap)
        request = ledger.getTransactionsRequest(Seq(party))
        sinceStart <- ledger.flatTransactions(request)
        sinceCp1 <- ledger.flatTransactions(request.update(_.begin := cp1))
        sinceCp2 <- ledger.flatTransactions(request.update(_.begin := cp2))
      } yield {
        val contractsSinceStart = sinceStart.flatMap(createdEvents).map(_.contractId).toSet
        val expectedContractsSinceStart = Set(first, second, third)
        assert(
          contractsSinceStart == expectedContractsSinceStart,
          s"Expected contracts since start: $expectedContractsSinceStart, actual result: $contractsSinceStart")

        val contractsSinceCp1 = sinceCp1.flatMap(createdEvents).map(_.contractId).toSet
        val expectedContractsSinceCp1 = Set(second, third)
        assert(
          contractsSinceCp1 == expectedContractsSinceCp1,
          s"Expected contracts since first checkpoint: $expectedContractsSinceCp1, actual result: $expectedContractsSinceCp1"
        )

        val contractsSinceCp2 = sinceCp2.flatMap(createdEvents).map(_.contractId).toSet
        val expectedContractsSinceCp2 = Set(third)
        assert(
          contractsSinceCp2 == expectedContractsSinceCp2,
          s"Expected contracts since second checkpoint: $expectedContractsSinceCp2, actual result: $expectedContractsSinceCp2"
        )
      }
  }

}
