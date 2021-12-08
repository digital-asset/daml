// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.deduplication

import java.util.UUID

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test.{Dummy, DummyWithAnnotation, TextKey, TextKeyOperations}
import com.daml.lf.data.Ref
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
    staticTime: Boolean,
) extends LedgerTestSuite {

  type OffsetWithCompletion = (LedgerOffset, Completion)

  val deduplicationDuration: FiniteDuration = scaledDuration(3.seconds)

  val ledgerWaitInterval: FiniteDuration = ledgerTimeInterval * 2
  val defaultDeduplicationWindowWait: FiniteDuration = deduplicationDuration + ledgerWaitInterval

  def deduplicationFeatures: DeduplicationFeatures

  protected def runWithDeduplicationDelay(
      participants: Seq[ParticipantTestContext]
  )(
      testWithDelayMechanism: DelayMechanism => Future[Unit]
  )(implicit ec: ExecutionContext): Future[Unit]

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
          _.commands.deduplicationPeriod :=
            DeduplicationPeriod.DeduplicationTime(deduplicationDuration.asProtobuf)
        )
      runWithDeduplicationDelay(configuredParticipants) { delay =>
        for {
          // Submit command (first deduplication window)
          // Note: the second submit() in this block is deduplicated and thus rejected by the ledger API server,
          // only one submission is therefore sent to the ledger.
          (_, completion1) <- submitRequestAndAssertCompletionAccepted(ledger)(request, party)
          _ <- submitRequestAndAssertDeduplication(ledger)(request, party)
          // Wait until the end of first deduplication window
          _ <- delay.delayForEntireDeduplicationPeriod()

          // Submit command (second deduplication window)
          // Note: the deduplication window is guaranteed to have passed on both
          // the ledger API server and the ledger itself, since the test waited more than
          // `deduplicationSeconds` after receiving the first command *completion*.
          // The first submit() in this block should therefore lead to an accepted transaction.
          (_, completion2) <- submitRequestAndAssertCompletionAccepted(ledger)(request, party)
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
      _ <- submitRequestAndAssertSyncFailure(ledger)(
        requestA,
        Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
      )

      // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
      _ <- submitRequestAndAssertSyncFailure(ledger)(
        requestA,
        Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
      )
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
      runWithDeduplicationDelay(configuredParticipants) { delay =>
        for {
          // Submit command (first deduplication window)
          _ <- ledger.submitAndWait(request)
          _ <- submitAndWaitRequestAndAssertDeduplication(ledger)(request)

          // Wait until the end of first deduplication window
          _ <- delay.delayForEntireDeduplicationPeriod()

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

  // staticTime - we run calls in parallel and with static time we would need to advance the time,
  //              therefore this cannot be run in static time
  if (!staticTime)
    testGivenAllParticipants(
      s"${testNamingPrefix}SimpleDeduplicationMixedClients",
      "Deduplicate commands within the deduplication time window using the command client and the command submission client",
      allocate(SingleParty),
    )(implicit ec =>
      configuredParticipants => { case Participants(Participant(ledger, party)) =>
        def generateVariations(elements: List[List[Boolean]]): List[List[Boolean]] =
          elements match {
            case Nil => List(Nil)
            case currentElement :: tail =>
              currentElement.flatMap(value => generateVariations(tail).map(value :: _))
          }

        runWithDeduplicationDelay(configuredParticipants) { delay =>
          {
            val numberOfCalls = 4
            Future // cover all the different generated variations of submit and submitAndWait
              .traverse(generateVariations(List.fill(numberOfCalls)(List(true, false)))) {
                case firstCall :: secondCall :: thirdCall :: fourthCall :: Nil =>
                  val submitAndWaitRequest = ledger
                    .submitAndWaitRequest(party, Dummy(party).create.command)
                    .update(
                      _.commands.deduplicationTime := deduplicationDuration.asProtobuf
                    )
                  val submitRequest = ledger
                    .submitRequest(party, Dummy(party).create.command)
                    .update(
                      _.commands.commandId := submitAndWaitRequest.getCommands.commandId,
                      _.commands.deduplicationTime := deduplicationDuration.asProtobuf,
                    )

                  def submitAndAssertAccepted(submitAndWait: Boolean) = {
                    if (submitAndWait) ledger.submitAndWait(submitAndWaitRequest)
                    else
                      submitRequestAndAssertCompletionAccepted(ledger)(submitRequest, party)
                        .map(_ => ())
                  }

                  def submitAndAssertDeduplicated(submitAndWait: Boolean) = {
                    if (submitAndWait)
                      submitAndWaitRequestAndAssertDeduplication(ledger)(submitAndWaitRequest)
                    else submitRequestAndAssertDeduplication(ledger)(submitRequest, party)
                  }

                  for {
                    // Submit command (first deduplication window)
                    _ <- submitAndAssertAccepted(firstCall)
                    _ <- submitAndAssertDeduplicated(secondCall)

                    // Wait until the end of first deduplication window
                    _ <- delay.delayForEntireDeduplicationPeriod()

                    // Submit command (second deduplication window)
                    _ <- submitAndAssertAccepted(thirdCall)
                    _ <- submitAndAssertDeduplicated(fourthCall)
                  } yield {}
                case _ => throw new IllegalArgumentException("Wrong call list constructed")
              }
              .flatMap { _ =>
                assertPartyHasActiveContracts(ledger)(
                  party = party,
                  noOfActiveContracts = 32, // 16 test cases, with 2 contracts per test case
                )
              }
          }
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

  testGivenAllParticipants(
    s"${testNamingPrefix}DeduplicateUsingOffsets",
    "Deduplicate commands within the deduplication period defined by the offset",
    allocate(SingleParty),
  )(implicit ec =>
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
      runWithDeduplicationDelay(configuredParticipants) { delay =>
        for {
          (offset1, _) <- submitRequestAndAssertCompletionAccepted(ledger)(request, party)
          // Wait for any ledgers that might adjust based on time skews
          // This is done so that we can validate that the third command is accepted
          _ <- delayForOffsetIfRequired(ledger, delay)
          // Submit command again using the first offset as the deduplication offset
          (offset2, _) <- submitRequestAndAssertAsyncDeduplication(ledger)(
            request.update(
              _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                Ref.HexString.assertFromString(offset1.getAbsolute)
              )
            ),
            party,
          )
          // Submit command again using the rejection offset as a deduplication period
          _ <- submitRequestAndAssertCompletionAccepted(ledger)(
            request.update(
              _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                Ref.HexString.assertFromString(offset2.getAbsolute)
              )
            ),
            party,
          )
        } yield {}
      }
    }
  )

  private def delayForOffsetIfRequired(
      participantTestContext: ParticipantTestContext,
      delayMechanism: DelayMechanism,
  )(implicit ec: ExecutionContext): Future[Unit] =
    deduplicationFeatures.deduplicationOffsetSupport match {
      case DeduplicationOffsetSupport.PassThroughOffsetSupport =>
        Future.successful(())
      case DeduplicationOffsetSupport.OffsetConversionToDurationSupport =>
        // the converted duration is calculated as the interval between submission time and offset record time
        // the duration is extended with maxSkew when determining if the command is a duplicate or not (pre-execution)
        participantTestContext
          .getTimeModel()
          .flatMap(response => {
            delayMechanism.delayBy(
              response.getTimeModel.getMaxSkew.asScala +
                response.getTimeModel.getMinSkew.asScala
            )
          })
    }

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
  )(request: SubmitRequest, parties: Party*)(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] = {
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
    submitRequestAndAssertSyncFailure(ledger)(
      request,
      Code.ALREADY_EXISTS,
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
    )

  private def submitRequestAndAssertSyncFailure(ledger: ParticipantTestContext)(
      request: SubmitRequest,
      code: Code,
      selfServiceErrorCode: ErrorCode,
  )(implicit ec: ExecutionContext) = ledger
    .submit(request)
    .mustFail(s"Request expected to fail with code $code")
    .map(
      assertGrpcError(
        ledger,
        _,
        code,
        selfServiceErrorCode,
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
          ledger,
          _,
          expectedCode = Code.ALREADY_EXISTS,
          selfServiceErrorCode = LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
          exceptionMessageSubstring = None,
          checkDefiniteAnswerMetadata = true,
        )
      )
  }

  protected def submitRequestAndAssertAsyncDeduplication(ledger: ParticipantTestContext)(
      request: SubmitRequest,
      parties: Party*
  )(implicit ec: ExecutionContext): Future[OffsetWithCompletion] =
    submitRequestAndAssertCompletionStatus(
      ledger
    )(request, Code.ALREADY_EXISTS, parties: _*)

  private def submitRequestAndAssertCompletionStatus(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, statusCode: Code, parties: Party*)(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitRequestAndFindCompletion(ledger)(request, parties: _*).map { case (offset, completion) =>
      assert(
        completion.getStatus.code == statusCode.value(),
        s"Expecting completion with status code $statusCode but completion has status ${completion.status}",
      )
      offset -> completion
    }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, parties: Party*)(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] = {
    val submissionId = UUID.randomUUID().toString
    submitRequest(ledger)(request.update(_.commands.submissionId := submissionId))
      .flatMap(ledgerEnd => {
        ledger
          .findCompletion(ledger.completionStreamRequest(ledgerEnd)(parties: _*))(
            _.submissionId == submissionId
          )
          .map(_.toList)
      })
      .map { completions =>
        assertSingleton("Expected only one completion", completions)
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
  trait DelayMechanism {
    val deduplicationDuration: Duration
    val extraWait: Duration

    /** Delay by the guaranteed full deduplication period. After calling this method any duplicate calls should succeed
      */
    def delayForEntireDeduplicationPeriod(): Future[Unit] =
      delayBy(deduplicationDuration + extraWait)

    def delayBy(duration: Duration): Future[Unit]
  }

  class TimeDelayMechanism(val deduplicationDuration: Duration, val extraWait: Duration)(implicit
      ec: ExecutionContext
  ) extends DelayMechanism {

    override def delayBy(duration: Duration): Future[Unit] = Delayed.by(duration)(())
  }

  class StaticTimeDelayMechanism(
      ledger: ParticipantTestContext,
      val deduplicationDuration: Duration,
      val extraWait: Duration,
  )(implicit ec: ExecutionContext)
      extends DelayMechanism {
    override def delayBy(duration: Duration): Future[Unit] =
      ledger
        .time()
        .flatMap { currentTime =>
          ledger.setTime(currentTime, currentTime.plusMillis(duration.toMillis))
        }
  }
  sealed trait DeduplicationOffsetSupport

  object DeduplicationOffsetSupport {
    case object PassThroughOffsetSupport extends DeduplicationOffsetSupport
    case object OffsetConversionToDurationSupport extends DeduplicationOffsetSupport
  }

  /** @param participantDeduplication If participant deduplication is enabled then we will receive synchronous rejections
    */
  case class DeduplicationFeatures(
      participantDeduplication: Boolean,
      deduplicationOffsetSupport: DeduplicationOffsetSupport,
  )
}
