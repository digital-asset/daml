// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test.TextKeyOperations._
import com.daml.ledger.test.model.Test._
import com.daml.timer.Delayed
import io.grpc.Status

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Random, Success}

final class CommandDeduplicationIT(timeoutScaleFactor: Double, ledgerTimeInterval: FiniteDuration)
    extends LedgerTestSuite {
  private val deduplicationTime = 3.seconds * timeoutScaleFactor match {
    case duration: FiniteDuration => duration
    case _ =>
      throw new IllegalArgumentException(s"Invalid timeout scale factor: $timeoutScaleFactor")
  }
  private val deduplicationWindowWait = deduplicationTime + ledgerTimeInterval * 2

  test(
    "CDSimpleDeduplicationBasic",
    "Deduplicate commands within the deduplication time window",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestA1 = ledger
      .submitRequest(party, DummyWithAnnotation(party, "First submission").create.command)
      .update(
        _.commands.deduplicationTime := deduplicationTime.asProtobuf
      )
    val requestA2 = ledger
      .submitRequest(party, DummyWithAnnotation(party, "Second submission").create.command)
      .update(
        _.commands.deduplicationTime := deduplicationTime.asProtobuf,
        _.commands.commandId := requestA1.commands.get.commandId,
      )

    for {
      // Submit command A (first deduplication window)
      // Note: the second submit() in this block is deduplicated and thus rejected by the ledger API server,
      // only one submission is therefore sent to the ledger.
      ledgerEnd1 <- ledger.currentEnd()
      _ <- ledger.submit(requestA1)
      failure1 <- ledger
        .submit(requestA1)
        .mustFail("submitting the first request for the second time")
      completions1 <- ledger.firstCompletions(ledger.completionStreamRequest(ledgerEnd1)(party))

      // Wait until the end of first deduplication window
      _ <- Delayed.by(deduplicationWindowWait)(())

      // Submit command A (second deduplication window)
      // Note: the deduplication window is guaranteed to have passed on both
      // the ledger API server and the ledger itself, since the test waited more than
      // `deduplicationSeconds` after receiving the first command *completion*.
      // The first submit() in this block should therefore lead to an accepted transaction.
      ledgerEnd2 <- ledger.currentEnd()
      _ <- ledger.submit(requestA2)
      failure2 <- ledger
        .submit(requestA2)
        .mustFail("submitting the second request for the second time")
      completions2 <- ledger.firstCompletions(ledger.completionStreamRequest(ledgerEnd2)(party))

      // Inspect created contracts
      activeContracts <- ledger.activeContracts(party)
    } yield {
      assertGrpcError(failure1, Status.Code.ALREADY_EXISTS, "")
      assertGrpcError(failure2, Status.Code.ALREADY_EXISTS, "")

      assert(ledgerEnd1 != ledgerEnd2)

      val completionCommandId1 =
        assertSingleton("Expected only one first completion", completions1.map(_.commandId))
      val completionCommandId2 =
        assertSingleton("Expected only one second completion", completions2.map(_.commandId))

      assert(
        completionCommandId1 == requestA1.commands.get.commandId,
        "The command ID of the first completion does not match the command ID of the submission",
      )
      assert(
        completionCommandId2 == requestA2.commands.get.commandId,
        "The command ID of the second completion does not match the command ID of the submission",
      )

      assert(
        activeContracts.size == 2,
        s"There should be 2 active contracts, but received $activeContracts",
      )
    }
  })

  test(
    "CDLargeSubmittersNumber",
    "Deduplicate commands with a large number of submitters",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    /** Postgres has a limit on the index row size of 2712.
      * We test that a large submitters number doesn't cause failure because of that limit.
      */
    for {
      // Need to manually allocate parties to avoid db string compression
      parties <- Future.traverse(1 to 50) { number =>
        ledger.allocateParty(
          partyIdHint =
            Some(s"deduplicationRandomParty_${number}_" + Random.alphanumeric.take(100).mkString),
          displayName = Some(s"Clone $number"),
        )
      }
      request = ledger
        .submitRequest(
          actAs = parties.toList,
          readAs = parties.toList,
          commands = DummyWithAnnotation(parties.head, "First submission").create.command,
        )
        .update(
          _.commands.deduplicationTime := deduplicationTime.asProtobuf
        )
      _ <- ledger.submit(request)
    } yield {
      ()
    }
  })

  test(
    "CDStopOnSubmissionFailure",
    "Stop deduplicating commands on submission failure",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    // Do not set the deduplication timeout.
    // The server will default to the maximum possible deduplication timeout.
    val requestA = ledger.submitRequest(alice, Dummy(bob).create.command)

    for {
      // Submit an invalid command (should fail with INVALID_ARGUMENT)
      failure1 <- ledger.submit(requestA).mustFail("submitting an invalid argument")

      // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
      failure2 <- ledger
        .submit(requestA)
        .mustFail("submitting an invalid argument for the second time")
    } yield {
      assertGrpcError(failure1, Status.Code.INVALID_ARGUMENT, "")
      assertGrpcError(failure2, Status.Code.INVALID_ARGUMENT, "")
    }
  })

  test(
    "CDStopOnCompletionFailure",
    "Stop deduplicating commands on completion failure",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val key = ledger.nextKeyId()

    for {
      // Create a helper and a text key
      ko <- ledger.create(party, TextKeyOperations(party))
      _ <- ledger.create(party, TextKey(party, key, List()))

      // Create two competing requests
      requestA = ledger.submitAndWaitRequest(
        party,
        ko.exerciseTKOFetchAndRecreate(party, Tuple2(party, key)).command,
      )
      requestB = ledger.submitAndWaitRequest(
        party,
        ko.exerciseTKOFetchAndRecreate(party, Tuple2(party, key)).command,
      )

      // Submit both requests in parallel.
      // Either both succeed (if one transaction is recorded faster than the other submission starts command interpretation, unlikely)
      // Or one submission is rejected (if one transaction is recorded during the call of lookupMaximumLedgerTime() in [[LedgerTimeHelper]], unlikely)
      // Or one transaction is rejected (this is what we want to test)
      submissionResults <- Future.traverse(List(requestA, requestB))(request =>
        ledger.submitAndWait(request).transform(result => Success(request -> result))
      )

      // Resubmit a failed command.
      // No matter what the rejection reason was (hopefully it was a rejected transaction),
      // a resubmission of exactly the same command should succeed.
      _ <- submissionResults
        .collectFirst { case (request, Failure(_)) => request }
        .fold(Future.unit)(request => ledger.submitAndWait(request))
    } yield {
      ()
    }
  })

  test(
    "CDSimpleDeduplicationCommandClient",
    "Deduplicate commands within the deduplication time window using the command client",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestA = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(
        _.commands.deduplicationTime := deduplicationTime.asProtobuf
      )

    for {
      // Submit command A (first deduplication window)
      _ <- ledger.submitAndWait(requestA)
      failure1 <- ledger
        .submitAndWait(requestA)
        .mustFail("submitting a request for the second time, in the first deduplication window")

      // Wait until the end of first deduplication window
      _ <- Delayed.by(deduplicationWindowWait)(())

      // Submit command A (second deduplication window)
      _ <- ledger.submitAndWait(requestA)
      failure2 <- ledger
        .submitAndWait(requestA)
        .mustFail("submitting a request for the second time, in the second deduplication window")

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
  })

  test(
    "CDDeduplicateSubmitterBasic",
    "Commands with identical submitter and command identifier should be deduplicated by the submission client",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger
      .submitRequest(bob, Dummy(bob).create.command)
      .update(_.commands.commandId := aliceRequest.getCommands.commandId)

    for {
      // Submit a command as alice
      _ <- ledger.submit(aliceRequest)
      failure1 <- ledger
        .submit(aliceRequest)
        .mustFail("submitting a request as Alice for the second time")

      // Submit another command that uses same commandId, but is submitted by Bob
      _ <- ledger.submit(bobRequest)
      failure2 <- ledger
        .submit(bobRequest)
        .mustFail("submitting the same request as Bob, for the second time")

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
  })

  test(
    "CDDeduplicateSubmitterCommandClient",
    "Commands with identical submitter and command identifier should be deduplicated by the command client",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger
      .submitAndWaitRequest(bob, Dummy(bob).create.command)
      .update(_.commands.commandId := aliceRequest.getCommands.commandId)

    for {
      // Submit a command as alice
      _ <- ledger.submitAndWait(aliceRequest)
      failure1 <- ledger
        .submitAndWait(aliceRequest)
        .mustFail("submitting a request as Alice for the second time")

      // Submit another command that uses same commandId, but is submitted by Bob
      _ <- ledger.submitAndWait(bobRequest)
      failure2 <- ledger
        .submitAndWait(bobRequest)
        .mustFail("submitting the same request as Bob, for the second time")

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
  })
}
