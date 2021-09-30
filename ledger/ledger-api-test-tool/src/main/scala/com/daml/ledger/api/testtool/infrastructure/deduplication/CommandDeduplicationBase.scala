// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.deduplication

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertGrpcError, assertSingleton, _}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase.DeduplicationFeatures
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test.{Dummy, DummyWithAnnotation, TextKey, TextKeyOperations}
import com.daml.timer.Delayed
import io.grpc.Status.Code

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@nowarn("msg=deprecated")
private[testtool] abstract class CommandDeduplicationBase(
    timeoutScaleFactor: Double,
    ledgerTimeInterval: FiniteDuration,
) extends LedgerTestSuite {

  val deduplicationDuration: FiniteDuration = scaledDuration(3.seconds)

  val ledgerWaitInterval: FiniteDuration = ledgerTimeInterval * 2
  val defaultDeduplicationWindowWait: FiniteDuration = deduplicationDuration + ledgerWaitInterval

  def deduplicationFeatures: DeduplicationFeatures

  protected def runGivenDeduplicationWait(
      participants: Seq[ParticipantTestContext]
  )(test: Duration => Future[Unit])(implicit
      ec: ExecutionContext
  ): Future[Unit]

  protected def testNamingPrefix: String

  testGivenAllParticipants(
    s"${testNamingPrefix}SimpleDeduplicationBasic",
    "Deduplicate commands within the deduplication duration window",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec =>
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
        .update(
          _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationTime(
            deduplicationDuration.asProtobuf
          )
        )
      runGivenDeduplicationWait(configuredParticipants) { deduplicationWait =>
        for {
          // Submit command (first deduplication window)
          // Note: the second submit() in this block is deduplicated and thus rejected by the ledger API server,
          // only one submission is therefore sent to the ledger.
          completion1 <- submitRequestAndAssertCompletionAccepted(ledger)(request, party)
          _ <- submitRequestAndAssertDeduplication(ledger)(request, party)
          // Wait until the end of first deduplication window
          _ <- Delayed.by(deduplicationWait)(())

          // Submit command (second deduplication window)
          // Note: the deduplication window is guaranteed to have passed on both
          // the ledger API server and the ledger itself, since the test waited more than
          // `deduplicationSeconds` after receiving the first command *completion*.
          // The first submit() in this block should therefore lead to an accepted transaction.
          completion2 <- submitRequestAndAssertCompletionAccepted(ledger)(request, party)
          _ <- submitRequestAndAssertDeduplication(ledger)(request, party)
          // Inspect created contracts
          _ <- assertPartyHasActiveContracts(ledger)(
            party = party,
            noOfActiveContracts = 2,
          )
        } yield {
          assert(
            completion1.commandId == request.commands.get.commandId,
            "The command ID of the first completion does not match the command ID of the submission",
          )
          assert(
            completion2.commandId == request.commands.get.commandId,
            "The command ID of the second completion does not match the command ID of the submission",
          )
        }
      }
    }
  )

  test(
    s"${testNamingPrefix}StopOnSubmissionFailure",
    "Stop deduplicating commands on submission failure",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    // Do not set the deduplication timeout.
    // The server will default to the maximum possible deduplication timeout.
    val requestA = ledger.submitRequest(alice, Dummy(bob).create.command)

    for {
      // Submit an invalid command (should fail with INVALID_ARGUMENT)
      _ <- submitRequestAndAssertSyncFailure(ledger)(requestA, Code.INVALID_ARGUMENT)

      // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
      _ <- submitRequestAndAssertSyncFailure(ledger)(requestA, Code.INVALID_ARGUMENT)
    } yield {}
  })

  test(
    s"${testNamingPrefix}StopOnCompletionFailure",
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

  testGivenAllParticipants(
    s"${testNamingPrefix}SimpleDeduplicationCommandClient",
    "Deduplicate commands within the deduplication time window using the command client",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec =>
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitAndWaitRequest(party, Dummy(party).create.command)
        .update(
          _.commands.deduplicationTime := deduplicationDuration.asProtobuf
        )
      runGivenDeduplicationWait(configuredParticipants) { deduplicationWait =>
        for {
          // Submit command (first deduplication window)
          _ <- ledger.submitAndWait(request)
          _ <- submitAndWaitRequestAndAssertDeduplication(ledger)(request)

          // Wait until the end of first deduplication window
          _ <- Delayed.by(deduplicationWait)(())

          // Submit command (second deduplication window)
          _ <- ledger.submitAndWait(request)
          _ <- submitAndWaitRequestAndAssertDeduplication(ledger)(request)

          // Inspect created contracts
          _ <- assertPartyHasActiveContracts(ledger)(
            party = party,
            noOfActiveContracts = 2,
          )
        } yield {}
      }
    }
  )

  test(
    s"${testNamingPrefix}DeduplicateSubmitterBasic",
    "Commands with identical submitter and command identifier should be deduplicated by the submission client",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger
      .submitRequest(bob, Dummy(bob).create.command)
      .update(_.commands.commandId := aliceRequest.getCommands.commandId)

    for {
      // Submit a command as alice
      _ <- submitRequestAndAssertCompletionAccepted(ledger)(aliceRequest, alice)
      _ <- submitRequestAndAssertDeduplication(ledger)(aliceRequest, alice)

      // Submit another command that uses same commandId, but is submitted by Bob
      _ <- submitRequestAndAssertCompletionAccepted(ledger)(bobRequest, bob)
      _ <- submitRequestAndAssertDeduplication(ledger)(bobRequest, bob)
      _ <- assertPartyHasActiveContracts(ledger)(
        party = alice,
        noOfActiveContracts = 1,
      )
      _ <- assertPartyHasActiveContracts(ledger)(
        party = bob,
        noOfActiveContracts = 1,
      )
    } yield {}
  })

  test(
    s"${testNamingPrefix}DeduplicateSubmitterCommandClient",
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
      _ <- submitAndWaitRequestAndAssertDeduplication(ledger)(aliceRequest)

      // Submit another command that uses same commandId, but is submitted by Bob
      _ <- ledger.submitAndWait(bobRequest)
      _ <- submitAndWaitRequestAndAssertDeduplication(ledger)(bobRequest)
      // Inspect the ledger state
      _ <- assertPartyHasActiveContracts(ledger)(
        party = alice,
        noOfActiveContracts = 1,
      )
      _ <- assertPartyHasActiveContracts(ledger)(
        party = bob,
        noOfActiveContracts = 1,
      )
    } yield {}
  })

  protected def assertPartyHasActiveContracts(
      ledger: ParticipantTestContext
  )(party: Party, noOfActiveContracts: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    ledger
      .activeContracts(party)
      .map(contracts =>
        assert(
          contracts.length == noOfActiveContracts,
          s"Expected $noOfActiveContracts active contracts for $party but found ${contracts.length} active contracts",
        )
      )
  }
  protected def submitRequestAndAssertCompletionAccepted(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, parties: Party*)(implicit ec: ExecutionContext): Future[Completion] = {
    submitRequestAndAssertCompletionStatus(ledger)(request, Code.OK, parties: _*)
  }

  protected def submitRequestAndAssertDeduplication(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, parties: Party*)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    if (deduplicationFeatures.participantDeduplication)
      submitRequestAndAssertSyncDeduplication(ledger, request)
    else
      submitRequestAndAssertAsyncDeduplication(ledger)(request, parties: _*)
        .map(_ => ())
  }

  protected def submitRequestAndAssertSyncDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
  )(implicit ec: ExecutionContext): Future[Unit] =
    submitRequestAndAssertSyncFailure(ledger)(request, Code.ALREADY_EXISTS)

  private def submitRequestAndAssertSyncFailure(ledger: ParticipantTestContext)(
      request: SubmitRequest,
      code: Code,
  )(implicit ec: ExecutionContext) = ledger
    .submit(request)
    .mustFail(s"Request expected to fail with code $code")
    .map(
      assertGrpcError(
        _,
        code,
        exceptionMessageSubstring = None,
        checkDefiniteAnswerMetadata = true,
      )
    )

  protected def submitAndWaitRequestAndAssertDeduplication(ledger: ParticipantTestContext)(
      request: SubmitAndWaitRequest
  )(implicit ec: ExecutionContext): Future[Unit] = {
    ledger
      .submitAndWait(request)
      .mustFail("Request was accepted but we were expecting it to fail with a duplicate error")
      .map(
        assertGrpcError(
          _,
          expectedCode = Code.ALREADY_EXISTS,
          exceptionMessageSubstring = None,
          checkDefiniteAnswerMetadata = true,
        )
      )
  }

  protected def submitRequestAndAssertAsyncDeduplication(ledger: ParticipantTestContext)(
      request: SubmitRequest,
      parties: Party*
  )(implicit ec: ExecutionContext): Future[Completion] = submitRequestAndAssertCompletionStatus(
    ledger
  )(request, Code.ALREADY_EXISTS, parties: _*)

  private def submitRequestAndAssertCompletionStatus(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, statusCode: Code, parties: Party*)(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    submitRequestAndFindCompletion(ledger)(request, parties: _*).map(completion => {
      assert(
        completion.getStatus.code == statusCode.value(),
        s"Expecting completion with status code $statusCode but completion has status ${completion.status}",
      )
      completion
    })

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, parties: Party*)(implicit ec: ExecutionContext): Future[Completion] = {
    val submissionId = UUID.randomUUID().toString
    submitRequest(ledger)(request.update(_.commands.submissionId := submissionId))
      .flatMap(ledgerEnd => {
        ledger.firstCompletions(ledger.completionStreamRequest(ledgerEnd)(parties: _*))
      })
      .map { completions =>
        val completion = assertSingleton("Expected only one completion", completions)
        // The [[Completion.submissionId]] is set only for append-only ledgers
        if (deduplicationFeatures.appendOnlySchema)
          assert(
            completion.submissionId == submissionId,
            s"Submission id is different for completion. Completion has submission id [${completion.submissionId}], request has submission id [$submissionId]",
          )
        completion
      }
  }

  protected def submitRequest(
      ledger: ParticipantTestContext
  )(request: SubmitRequest)(implicit ec: ExecutionContext): Future[LedgerOffset] = for {
    ledgerEnd <- ledger.currentEnd()
    _ <- ledger.submit(request)
  } yield {
    ledgerEnd
  }

  protected def scaledDuration(duration: FiniteDuration): FiniteDuration = asFiniteDuration(
    duration * timeoutScaleFactor
  )

  protected def asFiniteDuration(duration: Duration): FiniteDuration = duration match {
    case duration: FiniteDuration => duration
    case _ =>
      throw new IllegalArgumentException(s"Invalid timeout scale factor: $timeoutScaleFactor")
  }
}

object CommandDeduplicationBase {

  /** @param participantDeduplication If participant deduplication is enabled then we will receive synchronous rejections
    * @param appendOnlySchema For [[Completion]], the submission id and deduplication period are filled only for append only schemas
    * Therefore, we need to assert on those fields only if it's an append only schema
    */
  case class DeduplicationFeatures(
      participantDeduplication: Boolean,
      appendOnlySchema: Boolean,
  )
}
