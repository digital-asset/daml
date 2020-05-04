// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertGrpcError
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.test_stable.DA.Types.Tuple2
import com.daml.ledger.test_stable.Test.TextKeyOperations._
import com.daml.ledger.test_stable.Test._
import com.daml.timer.Delayed
import com.google.protobuf.duration.Duration
import io.grpc.Status

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

final class CommandDeduplication(session: LedgerSession) extends LedgerTestSuite(session) {

  /** A deduplicated submission can either
    * succeed (if the participant knows that the original submission has succeeded),
    * or fail with status ALREADY_EXISTS */
  private[this] def assertDeduplicated(result: Either[Throwable, Unit]): Unit = result match {
    case Left(e) => assertGrpcError(e, Status.Code.ALREADY_EXISTS, "")
    case Right(v) => ()
  }

  test(
    "CDSimpleDeduplication",
    "Deduplicate commands within the deduplication time window",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val deduplicationSeconds = 5
      val deduplicationTime = Duration.of(deduplicationSeconds.toLong, 0)
      val a = UUID.randomUUID.toString
      val b = UUID.randomUUID.toString
      val requestA = ledger
        .submitRequest(party, Dummy(party).create.command)
        .update(
          _.commands.deduplicationTime := deduplicationTime,
          _.commands.commandId := a,
        )
      val requestB = ledger
        .submitAndWaitRequest(party, Dummy(party).create.command)
        .update(_.commands.commandId := b)

      for {
        // Submit command A (first deduplication window)
        _ <- ledger.submit(requestA)
        failure1 <- ledger.submit(requestA).failed

        // Wait until the end of first deduplication window
        _ <- Delayed.by((deduplicationSeconds + 1).seconds)(())

        // Submit command A (second deduplication window)
        _ <- ledger.submit(requestA)
        failure2 <- ledger.submit(requestA).failed

        // Submit and wait for command B (to get a unique completion for the end of the test)
        _ <- ledger.submitAndWait(requestB)

        // Inspect created contracts
        activeContracts <- ledger.activeContracts(party)
      } yield {
        assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
        assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

        assert(
          activeContracts.size == 3,
          s"There should be 3 active contracts, but received $activeContracts",
        )
      }
  }

  test(
    "CDStopOnSubmissionFailure",
    "Stop deduplicating commands on submission failure",
    allocate(TwoParties),
  ) {
    case Participants(Participant(ledger, alice, bob)) =>
      // Do not set the deduplication timeout.
      // The server will default to the maximum possible deduplication timeout.
      val requestA = ledger.submitRequest(alice, Dummy(bob).create.command)

      for {
        // Submit an invalid command (should fail with INVALID_ARGUMENT)
        failure1 <- ledger.submit(requestA).failed

        // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
        failure2 <- ledger.submit(requestA).failed
      } yield {
        assertGrpcError(failure1, Status.Code.INVALID_ARGUMENT, "")
        assertGrpcError(failure2, Status.Code.INVALID_ARGUMENT, "")
      }
  }

  test(
    "CDStopOnCompletionFailure",
    "Stop deduplicating commands on completion failure",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val key = UUID.randomUUID().toString
      val commandId = UUID.randomUUID().toString

      for {
        // Create a helper and a text key
        ko <- ledger.create(party, TextKeyOperations(party))
        _ <- ledger.create(party, TextKey(party, key, List()))

        // Create two competing requests
        requestTemplate = ledger.submitAndWaitRequest(
          party,
          ko.exerciseTKOFetchAndRecreate(party, Tuple2(party, key)).command)
        requestA = requestTemplate.update(_.commands.commandId := commandId + "-A")
        requestB = requestTemplate.update(_.commands.commandId := commandId + "-B")

        // Submit both requests in parallel.
        // Either both succeed (if one transaction is recorded faster than the other submission starts command interpretation, unlikely)
        // Or one submission is rejected (if one transaction is recorded during the call of lookupMaximumLedgerTime() in [[LedgerTimeHelper]], unlikely)
        // Or one transaction is rejected (this is what we want to test)
        submissionResults <- Future.traverse(List(requestA, requestB))(request =>
          ledger.submitAndWait(request).transform(result => Success(request -> result)))

        // Resubmit a failed command.
        // No matter what the rejection reason was (hopefully it was a rejected transaction),
        // a resubmission of exactly the same command should succeed.
        _ <- submissionResults
          .collectFirst { case (request, Failure(_)) => request }
          .fold(Future.successful(()))(request => ledger.submitAndWait(request))
      } yield {
        ()
      }
  }

  test(
    "CDSimpleDeduplicationCommandClient",
    "Deduplicate commands within the deduplication time window using the command client",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val deduplicationSeconds = 5
      val deduplicationTime = Duration.of(deduplicationSeconds.toLong, 0)
      val a = UUID.randomUUID.toString
      val requestA = ledger
        .submitAndWaitRequest(party, Dummy(party).create.command)
        .update(
          _.commands.deduplicationTime := deduplicationTime,
          _.commands.commandId := a,
        )

      for {
        // Submit command A (first deduplication window)
        _ <- ledger.submitAndWait(requestA)
        failure1 <- ledger.submitAndWait(requestA).failed

        // Wait until the end of first deduplication window
        _ <- Delayed.by((deduplicationSeconds + 1).seconds)(())

        // Submit command A (second deduplication window)
        _ <- ledger.submitAndWait(requestA)
        failure2 <- ledger.submitAndWait(requestA).failed

        // Inspect created contracts
        activeContracts <- ledger.activeContracts(party)
      } yield {
        assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
        assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

        assert(
          activeContracts.size == 2,
          s"There should be 2 active contracts, but received $activeContracts",
        )
      }
  }

  test(
    "CDDeduplicateSubmitter",
    "Commands with identical submitter and command identifier should be deduplicated by the submission client",
    allocate(TwoParties),
  ) {
    case Participants(Participant(ledger, alice, bob)) =>
      val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
      val bobRequest = ledger
        .submitRequest(bob, Dummy(bob).create.command)
        .update(_.commands.commandId := aliceRequest.getCommands.commandId)

      for {
        // Submit a command as alice
        _ <- ledger.submit(aliceRequest)
        failure1 <- ledger.submit(aliceRequest).failed

        // Submit another command that uses same commandId, but is submitted by Bob
        _ <- ledger.submit(bobRequest)
        failure2 <- ledger.submit(bobRequest).failed

        // Wait for command completions and inspect the ledger state
        _ <- ledger.firstCompletions(alice)
        _ <- ledger.firstCompletions(bob)
        aliceContracts <- ledger.activeContracts(alice)
        bobContracts <- ledger.activeContracts(bob)
      } yield {
        assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
        assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

        assert(
          aliceContracts.length == 1,
          s"Only one contract was expected to be seen by $alice but ${aliceContracts.length} appeared",
        )

        assert(
          bobContracts.length == 1,
          s"Only one contract was expected to be seen by $bob but ${bobContracts.length} appeared",
        )
      }
  }

  test(
    "CDDeduplicateSubmitterCommandClient",
    "Commands with identical submitter and command identifier should be deduplicated by the command client",
    allocate(TwoParties),
  ) {
    case Participants(Participant(ledger, alice, bob)) =>
      val aliceRequest = ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      val bobRequest = ledger
        .submitAndWaitRequest(bob, Dummy(bob).create.command)
        .update(_.commands.commandId := aliceRequest.getCommands.commandId)

      for {
        // Submit a command as alice
        _ <- ledger.submitAndWait(aliceRequest)
        failure1 <- ledger.submitAndWait(aliceRequest).failed

        // Submit another command that uses same commandId, but is submitted by Bob
        _ <- ledger.submitAndWait(bobRequest)
        failure2 <- ledger.submitAndWait(bobRequest).failed

        // Inspect the ledger state
        aliceContracts <- ledger.activeContracts(alice)
        bobContracts <- ledger.activeContracts(bob)
      } yield {
        assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
        assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

        assert(
          aliceContracts.length == 1,
          s"Only one contract was expected to be seen by $alice but ${aliceContracts.length} appeared",
        )

        assert(
          bobContracts.length == 1,
          s"Only one contract was expected to be seen by $bob but ${bobContracts.length} appeared",
        )
      }
  }

}
