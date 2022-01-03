// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.deduplication

import java.util

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase._
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.version_service.DeduplicationPeriodSupport.OffsetSupport
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test.{Dummy, DummyWithAnnotation, TextKey, TextKeyOperations}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.SubmissionId
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
        val acceptedSubmissionId1 = newSubmissionId()
        val acceptedSubmissionId2 = newSubmissionId()

        for {
          // Submit command (first deduplication window)
          // Note: the second submit() in this block is deduplicated and thus rejected by the ledger API server,
          // only one submission is therefore sent to the ledger.
          (offset1, completion1) <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId1),
            party,
          )
          _ <- submitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(request),
            acceptedSubmissionId1,
            offset1,
            party,
          )
          // Wait until the end of first deduplication window
          _ <- delay.delayForEntireDeduplicationPeriod()

          // Submit command (second deduplication window)
          // Note: the deduplication window is guaranteed to have passed on both
          // the ledger API server and the ledger itself, since the test waited more than
          // `deduplicationSeconds` after receiving the first command *completion*.
          // The first submit() in this block should therefore lead to an accepted transaction.
          (offset2, completion2) <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId2),
            party,
          )
          _ <- submitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(request),
            acceptedSubmissionId2,
            offset2,
            party,
          )
          // Inspect created contracts
          _ <- assertPartyHasActiveContracts(
            ledger,
            party,
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
      _ <- submitRequestAndAssertSyncFailure(
        ledger,
        requestA,
        Code.INVALID_ARGUMENT,
        LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
      )

      // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
      _ <- submitRequestAndAssertSyncFailure(
        ledger,
        updateWithFreshSubmissionId(requestA),
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
        .fold(Future.unit)(request => ledger.submitAndWait(updateWithFreshSubmissionId(request)))
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
      val acceptedSubmissionId1 = newSubmissionId()
      val acceptedSubmissionId2 = newSubmissionId()
      runWithDeduplicationDelay(configuredParticipants) { delay =>
        for {
          // Submit command (first deduplication window)
          (completionOffset1, _) <- submitAndWaitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId1),
            party,
          )
          _ <- submitAndWaitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(request),
            acceptedSubmissionId1,
            completionOffset1,
          )

          // Wait until the end of first deduplication window
          _ <- delay.delayForEntireDeduplicationPeriod()

          // Submit command (second deduplication window)
          (completionOffset2, _) <- submitAndWaitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId2),
            party,
          )
          _ <- submitAndWaitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(request),
            acceptedSubmissionId2,
            completionOffset2,
          )

          // Inspect created contracts
          _ <- assertPartyHasActiveContracts(
            ledger,
            party,
            noOfActiveContracts = 2,
          )
        } yield {}
      }
    }
  )

  // staticTime - we run calls in parallel and with static time we would need to advance the time,
  //              therefore this cannot be run in static time
  testGivenAllParticipants(
    s"${testNamingPrefix}SimpleDeduplicationMixedClients",
    "Deduplicate commands within the deduplication time window using the command client and the command submission client",
    allocate(SingleParty),
    enabled = _ => !staticTime,
    disabledReason = "Cannot work in static time as we run multiple test cases in parallel",
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

                def submitAndAssertAccepted(
                    submitAndWait: Boolean,
                    acceptedSubmissionId: SubmissionId,
                ): Future[LedgerOffset] = {
                  if (submitAndWait)
                    submitAndWaitRequestAndAssertCompletionAccepted(
                      ledger,
                      updateSubmissionId(submitAndWaitRequest, acceptedSubmissionId),
                      party,
                    )
                      .map(_._1)
                  else
                    submitRequestAndAssertCompletionAccepted(
                      ledger,
                      updateSubmissionId(submitRequest, acceptedSubmissionId),
                      party,
                    )
                      .map(_._1)
                }

                def submitAndAssertDeduplicated(
                    submitAndWait: Boolean,
                    acceptedSubmissionId: SubmissionId,
                    acceptedLedgerOffset: LedgerOffset,
                ): Future[Unit] =
                  if (submitAndWait)
                    submitAndWaitRequestAndAssertDeduplication(
                      ledger,
                      updateWithFreshSubmissionId(submitAndWaitRequest),
                      acceptedSubmissionId,
                      acceptedLedgerOffset,
                    )
                  else
                    submitRequestAndAssertDeduplication(
                      ledger,
                      updateWithFreshSubmissionId(submitRequest),
                      acceptedSubmissionId,
                      acceptedLedgerOffset,
                      party,
                    )

                val acceptedSubmissionId1 = newSubmissionId()
                val acceptedSubmissionId2 = newSubmissionId()
                for {
                  // Submit command (first deduplication window)
                  ledgerOffset1 <- submitAndAssertAccepted(firstCall, acceptedSubmissionId1)
                  _ <- submitAndAssertDeduplicated(
                    secondCall,
                    acceptedSubmissionId1,
                    ledgerOffset1,
                  )

                  // Wait until the end of first deduplication window
                  _ <- delay.delayForEntireDeduplicationPeriod()

                  // Submit command (second deduplication window)
                  ledgerOffset2 <- submitAndAssertAccepted(thirdCall, acceptedSubmissionId2)
                  _ <- submitAndAssertDeduplicated(
                    fourthCall,
                    acceptedSubmissionId2,
                    ledgerOffset2,
                  )
                } yield {}
              case _ => throw new IllegalArgumentException("Wrong call list constructed")
            }
            .flatMap { _ =>
              assertPartyHasActiveContracts(
                ledger,
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

    val aliceAcceptedSubmissionId = newSubmissionId()
    val bobAcceptedSubmissionId = newSubmissionId()

    for {
      // Submit a command as alice
      (aliceCompletionOffset, _) <- submitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(aliceRequest, aliceAcceptedSubmissionId),
        alice,
      )
      _ = println(s"Alice completion offset: $aliceCompletionOffset")
      _ <- submitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(aliceRequest),
        aliceAcceptedSubmissionId,
        aliceCompletionOffset,
        alice,
      )

      // Submit another command that uses same commandId, but is submitted by Bob
      (bobCompletionOffset, _) <- submitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(bobRequest, bobAcceptedSubmissionId),
        bob,
      )
      _ = println(s"Bob completion offset: $aliceCompletionOffset")
      _ <- submitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(bobRequest),
        bobAcceptedSubmissionId,
        bobCompletionOffset,
        bob,
      )
      _ <- assertPartyHasActiveContracts(
        ledger,
        party = alice,
        noOfActiveContracts = 1,
      )
      _ <- assertPartyHasActiveContracts(
        ledger,
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

    val aliceAcceptedSubmissionId = newSubmissionId()
    val bobAcceptedSubmissionId = newSubmissionId()
    for {
      // Submit a command as alice
      (aliceCompletionOffset, _) <- submitAndWaitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(aliceRequest, aliceAcceptedSubmissionId),
        alice,
      )
      _ <- submitAndWaitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(aliceRequest),
        aliceAcceptedSubmissionId,
        aliceCompletionOffset,
      )

      // Submit another command that uses same commandId, but is submitted by Bob
      (bobCompletionOffset, _) <- submitAndWaitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(bobRequest, bobAcceptedSubmissionId),
        bob,
      )
      _ <- submitAndWaitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(bobRequest),
        bobAcceptedSubmissionId,
        bobCompletionOffset,
      )
      // Inspect the ledger state
      _ <- assertPartyHasActiveContracts(
        ledger,
        party = alice,
        noOfActiveContracts = 1,
      )
      _ <- assertPartyHasActiveContracts(
        ledger,
        party = bob,
        noOfActiveContracts = 1,
      )
    } yield {}
  })

  testGivenAllParticipants(
    s"${testNamingPrefix}DeduplicateUsingOffsets",
    "Deduplicate commands within the deduplication period defined by the offset",
    allocate(SingleParty),
    runConcurrently = false, // updates the time model
  )(implicit ec =>
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
      val acceptedSubmissionId = newSubmissionId()
      runWithDeduplicationDelay(configuredParticipants) { delay =>
        for {
          (offset1, _) <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId),
            party,
          )
          // Wait for any ledgers that might adjust based on time skews
          // This is done so that we can validate that the third command is accepted
          _ <- delayForOffsetIfRequired(ledger, delay, ledger.features)
          // Submit command again using the first offset as the deduplication offset
          (_, _) <- submitRequestAndAssertAsyncDeduplication(
            ledger,
            updateWithFreshSubmissionId(
              request.update(
                _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                  Ref.HexString.assertFromString(offset1.getAbsolute)
                )
              )
            ),
            acceptedSubmissionId,
            offset1,
            party,
          )
          (offset3, _) <- submitRequestAndAssertCompletionAccepted(
            ledger,
            ledger.submitRequest(party, Dummy(party).create.command),
            party,
          )
          // Submit command again using the rejection offset as a deduplication period
          _ <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateWithFreshSubmissionId(
              request.update(
                _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                  Ref.HexString.assertFromString(offset3.getAbsolute)
                )
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
      features: Features,
  )(implicit ec: ExecutionContext): Future[Unit] =
    features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport match {
      case OffsetSupport.OFFSET_NATIVE_SUPPORT =>
        Future.unit
      case OffsetSupport.OFFSET_CONVERT_TO_DURATION =>
        // the converted duration is calculated as the interval between submission time
        // and offset record time + minSkew (used to determine maxRecordTime)
        //
        // the duration is extended with up to minSkew + maxSkew when using pre-execution,
        // as we use maxRecordTime and minRecordTime to calculate the interval between the two commands
        participantTestContext
          .getTimeModel()
          .flatMap(response => {
            delayMechanism.delayBy(
              response.getTimeModel.getMaxSkew.asScala +
                2 * response.getTimeModel.getMinSkew.asScala
            )
          })
      case OffsetSupport.Unrecognized(_) | OffsetSupport.OFFSET_NOT_SUPPORTED =>
        Future.unit
    }

  protected def assertPartyHasActiveContracts(
      ledger: ParticipantTestContext,
      party: Party,
      noOfActiveContracts: Int,
  )(implicit ec: ExecutionContext): Future[Unit] = {
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
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitRequestAndAssertCompletion(ledger, request, parties: _*) { completion =>
      assertCompletionStatus(request.toString, completion, Code.OK)
    }

  protected def submitAndWaitRequestAndAssertCompletionAccepted(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitAndWaitRequestAndAssertCompletion(ledger, request, parties: _*) { completion =>
      assertCompletionStatus(request.toString, completion, Code.OK)
    }

  protected def submitRequestAndAssertDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: LedgerOffset,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    if (deduplicationFeatures.participantDeduplication)
      submitRequestAndAssertSyncDeduplication(ledger, request, acceptedSubmissionId, acceptedOffset)
    else
      submitRequestAndAssertAsyncDeduplication(
        ledger,
        request,
        acceptedSubmissionId,
        acceptedOffset,
        parties: _*
      ).map(_ => ())

  protected def submitRequestAndAssertSyncDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: LedgerOffset,
  )(implicit ec: ExecutionContext): Future[Unit] =
    submitRequestAndAssertSyncFailure(
      ledger,
      request,
      Code.ALREADY_EXISTS,
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
      assertDeduplicatedSubmissionIdAndOffsetOnError(
        acceptedSubmissionId,
        acceptedOffset,
        _,
      ),
    )

  private def submitRequestAndAssertSyncFailure(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      code: Code,
      selfServiceErrorCode: ErrorCode,
      additionalErrorAssertions: Throwable => Unit = _ => (),
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger
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
          additionalErrorAssertions,
        )
      )

  protected def submitAndWaitRequestAndAssertDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: LedgerOffset,
  )(implicit ec: ExecutionContext): Future[Unit] =
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
          assertDeduplicatedSubmissionIdAndOffsetOnError(
            acceptedSubmissionId,
            acceptedOffset,
            _,
          ),
        )
      )

  protected def submitRequestAndAssertAsyncDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: LedgerOffset,
      parties: Party*
  )(implicit ec: ExecutionContext): Future[OffsetWithCompletion] =
    submitRequestAndAssertCompletion(
      ledger,
      request,
      parties: _*
    ) { completion =>
      assertCompletionStatus(request.toString, completion, Code.ALREADY_EXISTS)
      assertDeduplicatedSubmissionIdAndOffsetOnCompletion(
        acceptedSubmissionId,
        acceptedOffset,
        completion,
      )
    }

  private def assertCompletionStatus(
      requestString: String,
      completion: Completion,
      statusCode: Code,
  ): Unit =
    assert(
      completion.getStatus.code == statusCode.value(),
      s"""Expecting completion with status code $statusCode but completion has status ${completion.status}.
         |Request: $requestString
         |Completion: $completion
         |Metadata: ${extractErrorInfoMetadata(
        GrpcStatus.toJavaProto(completion.getStatus)
      )}""".stripMargin,
    )

  private def assertDeduplicatedSubmissionIdAndOffsetOnError(
      acceptedSubmissionId: SubmissionId,
      acceptedCompletionOffset: LedgerOffset,
      t: Throwable,
  ): Unit = t match {
    case exception: Exception =>
      val metadata = extractErrorInfoMetadata(exception)
      assertExistingSubmissionIdOnMetadata(metadata, acceptedSubmissionId)
      assertExistingCompletionOffsetOnMetadata(metadata, acceptedCompletionOffset)
    case _ => ()
  }

  private def assertDeduplicatedSubmissionIdAndOffsetOnCompletion(
      acceptedSubmissionId: SubmissionId,
      acceptedCompletionOffset: LedgerOffset,
      completion: Completion,
  ): Unit = {
    val metadata = extractErrorInfoMetadata(
      GrpcStatus.toJavaProto(completion.getStatus)
    )
    assertExistingSubmissionIdOnMetadata(metadata, acceptedSubmissionId)
    assertExistingCompletionOffsetOnMetadata(metadata, acceptedCompletionOffset)
  }

  private def assertExistingSubmissionIdOnMetadata(
      metadata: util.Map[String, String],
      acceptedSubmissionId: SubmissionId,
  ): Unit =
    Option(metadata.get("existing_submission_id")).foreach { metadataExistingSubmissionId =>
      assertEquals(
        "submission ID mismatch",
        metadataExistingSubmissionId,
        acceptedSubmissionId,
      )
    }

  private def assertExistingCompletionOffsetOnMetadata(
      metadata: util.Map[String, String],
      acceptedCompletionOffset: LedgerOffset,
  ): Unit =
    Option(metadata.get("completion_offset")).foreach { offset =>
      assertEquals(
        "completion offset mismatch",
        absoluteLedgerOffset(offset),
        acceptedCompletionOffset,
      )
    }

  private def submitRequestAndAssertCompletion(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(
      additionalCompletionAssertion: Completion => Unit
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitRequestAndFindCompletion(ledger, request, parties: _*).map { case (offset, completion) =>
      additionalCompletionAssertion(completion)
      offset -> completion
    }

  private def submitAndWaitRequestAndAssertCompletion(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(
      additionalCompletionAssertion: Completion => Unit
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitRequestAndFindCompletion(ledger, request, parties: _*).map { case (offset, completion) =>
      additionalCompletionAssertion(completion)
      offset -> completion
    }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    submitRequest(ledger)(request)
      .flatMap(ledgerEnd => {
        ledger
          .findCompletion(ledger.completionStreamRequest(ledgerEnd)(parties: _*)) { completion =>
            request.commands.map(_.submissionId).contains(completion.submissionId)
          }
          .map(_.toList)
      })
      .map { completions =>
        assertSingleton("Expected only one completion", completions)
      }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[OffsetWithCompletion] =
    ledger
      .submitAndWait(request)
      .flatMap { _ =>
        ledger
          .findCompletion(ledger.completionStreamRequest()(parties: _*)) { completion =>
            request.commands.map(_.submissionId).contains(completion.submissionId)
          }
          .map(_.toList)
      }
      .map { completions =>
        assert(completions.head._1.getAbsolute.nonEmpty, "Expected a populated completion offset")
        assertSingleton("Expected only one completion", completions)
      }

  protected def submitRequest(
      ledger: ParticipantTestContext
  )(
      request: SubmitRequest
  )(implicit ec: ExecutionContext): Future[LedgerOffset] = for {
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

  private def absoluteLedgerOffset(value: String) =
    LedgerOffset(LedgerOffset.Value.Absolute(value))

  private def updateSubmissionId(
      request: SubmitRequest,
      submissionId: SubmissionId,
  ): SubmitRequest =
    request.update(_.commands.submissionId := submissionId)

  private def updateSubmissionId(
      request: SubmitAndWaitRequest,
      acceptedSubmissionId1: SubmissionId,
  ): SubmitAndWaitRequest =
    request.update(_.commands.submissionId := acceptedSubmissionId1)

  private def updateWithFreshSubmissionId(request: SubmitRequest): SubmitRequest =
    request.update(_.commands.submissionId := newSubmissionId())

  private def updateWithFreshSubmissionId(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    request.update(_.commands.submissionId := newSubmissionId())

  private def newSubmissionId(): SubmissionId = SubmissionIdGenerator.Random.generate()
}

object CommandDeduplicationBase {
  type OffsetWithCompletion = (LedgerOffset, Completion)

  trait DelayMechanism {
    val deduplicationDuration: Duration
    val extraWait: Duration

    /** Delay by the guaranteed full deduplication period. After calling this method any duplicate calls should succeed
      */
    def delayForEntireDeduplicationPeriod(): Future[Unit] =
      delayBy(deduplicationDuration + extraWait)

    def delayBy(duration: Duration): Future[Unit]
  }

  class TimeDelayMechanism(
      val deduplicationDuration: Duration,
      val extraWait: Duration,
  )(implicit ec: ExecutionContext)
      extends DelayMechanism {
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

  /** @param participantDeduplication If participant deduplication is enabled then we will receive synchronous rejections
    */
  case class DeduplicationFeatures(
      participantDeduplication: Boolean
  )
}
