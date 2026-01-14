// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.FutureAssertions.*
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters.*
import com.daml.ledger.api.testtool.infrastructure.assertions.CommandDeduplicationAssertions.{
  assertDeduplicationDuration,
  assertDeduplicationOffset,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.time.{DelayMechanism, Durations}
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.completion.Completion.DeduplicationPeriod as CompletionDeduplicationPeriod
import com.daml.ledger.test.java.model.test.{Dummy, DummyWithAnnotation}
import com.daml.logging.LoggingContext
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, SubmissionId}
import io.grpc.Status.Code

import java.time
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

final class CommandDeduplicationIT(
    timeoutScaleFactor: Double
) extends LedgerTestSuite {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val deduplicationDuration: FiniteDuration =
    Durations.scaleDuration(2.seconds, timeoutScaleFactor)

  test(
    s"SimpleDeduplicationBasic",
    "Deduplicate commands within the deduplication duration window",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger
      .submitRequest(party, new DummyWithAnnotation(party, "Duplicate command").create.commands)
      .update(
        _.commands.deduplicationPeriod :=
          DeduplicationPeriod.DeduplicationDuration(deduplicationDuration.asProtobuf)
      )
    val firstAcceptedSubmissionId = newSubmissionId()
    for {
      // Submit command (first deduplication window)
      // Note: the second submit() in this block is deduplicated and thus rejected by the ledger API server,
      // only one submission is therefore sent to the ledger.
      response <- submitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(request, firstAcceptedSubmissionId),
        party,
      )
      _ <- submitRequestAndAssertDeduplication(
        ledger,
        request,
        firstAcceptedSubmissionId,
        response.offset,
        party,
      )
      // Inspect created contracts
      _ <- assertPartyHasActiveContracts(
        ledger,
        party,
        noOfActiveContracts = 1,
      )
    } yield {
      assert(
        response.commandId == request.commands.value.commandId,
        "The command ID of the first completion does not match the command ID of the submission",
      )
    }
  })

  test(
    s"StopOnSubmissionFailure",
    "Stop deduplicating commands on submission failure",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    // Do not set the deduplication timeout.
    // The server will default to the maximum possible deduplication timeout.
    val requestA = ledger.submitRequest(alice, new Dummy(bob).create.commands)

    for {
      // Submit an invalid command (should fail with INVALID_ARGUMENT)
      _ <- submitRequestAndAssertSyncFailure(
        ledger,
        requestA,
        Code.INVALID_ARGUMENT,
        CommandExecutionErrors.Interpreter.AuthorizationError,
      )

      // Re-submit the invalid command (should again fail with INVALID_ARGUMENT and not with ALREADY_EXISTS)
      _ <- submitRequestAndAssertSyncFailure(
        ledger,
        updateWithFreshSubmissionId(requestA),
        Code.INVALID_ARGUMENT,
        CommandExecutionErrors.Interpreter.AuthorizationError,
      )
    } yield {}
  })

  test(
    s"SimpleDeduplicationCommandClient",
    "Deduplicate commands within the deduplication time window using the command client",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger
      .submitAndWaitRequest(party, new Dummy(party).create.commands)
      .update(
        _.commands.deduplicationDuration := deduplicationDuration.asProtobuf
      )
    val acceptedSubmissionId1 = newSubmissionId()
    for {
      // Submit command (first deduplication window)
      response <- submitAndWaitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(request, acceptedSubmissionId1),
        party,
      )
      _ <- submitAndWaitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(request),
        acceptedSubmissionId1,
        response.offset,
      )

      // Inspect created contract
      _ <- assertPartyHasActiveContracts(
        ledger,
        party,
        noOfActiveContracts = 1,
      )
    } yield {}
  })

  // staticTime - we run calls in parallel and with static time we would need to advance the time,
  //              therefore this cannot be run in static time
  testGivenAllParticipants(
    "DeduplicationMixedClients",
    "Deduplicate commands within the deduplication time window using the command client and the command submission client",
    allocate(Parties(16)),
    enabled = !_.staticTime,
    disabledReason = "Cannot work in static time as we run multiple test cases in parallel",
    runConcurrently = false, // updates the time model
    timeoutScale = 3,
  )(implicit ec =>
    _ => { case Participants(Participant(ledger, parties)) =>
      def generateVariations(elements: List[List[Boolean]]): List[List[Boolean]] =
        elements match {
          case Nil => List(Nil)
          case currentElement :: tail =>
            currentElement.flatMap(value => generateVariations(tail).map(value :: _))
        }

      runWithTimeModel { minMaxSkewSum =>
        val numberOfCalls = 4
        // cover all the different generated variations of submit and submitAndWait
        val allGeneratedVariations =
          generateVariations(List.fill(numberOfCalls)(List(true, false))).zip(parties)
        forAllParallel(allGeneratedVariations) {
          case (firstCall :: secondCall :: thirdCall :: fourthCall :: Nil, party) =>
            mixedClientsCommandDeduplicationTestCase(
              ledger,
              party,
              ledger.delayMechanism,
              minMaxSkewSum,
            )(
              firstCall,
              secondCall,
              thirdCall,
              fourthCall,
            )
          case _ => throw new IllegalArgumentException("Wrong call list constructed")
        }
          .map(_ => ())
      }
    }
  )

  private def mixedClientsCommandDeduplicationTestCase(
      ledger: ParticipantTestContext,
      party: Party,
      delay: DelayMechanism,
      skews: FiniteDuration,
  )(firstCall: Boolean, secondCall: Boolean, thirdCall: Boolean, fourthCall: Boolean)(implicit
      ec: ExecutionContext
  ) = {
    val submitAndWaitRequest = ledger
      .submitAndWaitRequest(party, new Dummy(party).create.commands)
      .update(
        _.commands.deduplicationDuration := deduplicationDuration.asProtobuf
      )
    val submitRequest = ledger
      .submitRequest(party, new Dummy(party).create.commands)
      .update(
        _.commands.commandId := submitAndWaitRequest.getCommands.commandId,
        _.commands.deduplicationDuration := deduplicationDuration.asProtobuf,
      )

    def submitAndAssertAccepted(
        submitAndWait: Boolean
    ): Future[Completion] = {
      val acceptedSubmissionId: SubmissionId = newSubmissionId()
      if (submitAndWait)
        submitAndWaitRequestAndAssertCompletionAccepted(
          ledger,
          updateSubmissionId(submitAndWaitRequest, acceptedSubmissionId),
          party,
        )
      else
        submitRequestAndAssertCompletionAccepted(
          ledger,
          updateSubmissionId(submitRequest, acceptedSubmissionId),
          party,
        )
    }

    def submitAndAssertDeduplicated(
        submitAndWait: Boolean,
        acceptedSubmissionId: SubmissionId,
        acceptedParticipantOffset: Long,
    ): Future[Option[Completion]] =
      if (submitAndWait)
        submitAndWaitRequestAndAssertDeduplication(
          ledger,
          updateWithFreshSubmissionId(submitAndWaitRequest),
          acceptedSubmissionId,
          acceptedParticipantOffset,
        ).map(_ => None)
      else
        submitRequestAndAssertDeduplication(
          ledger,
          updateWithFreshSubmissionId(submitRequest),
          acceptedSubmissionId,
          acceptedParticipantOffset,
          party,
        ).map(Some(_))

    for {
      // Submit command (first deduplication window)
      firstAcceptedCommand <- submitAndAssertAccepted(firstCall)
      duplicateResponse <- submitAndAssertDeduplicated(
        secondCall,
        LedgerString.assertFromString(firstAcceptedCommand.submissionId),
        firstAcceptedCommand.offset,
      )
      deduplicationDurationFromPeriod = extractDurationFromDeduplicationPeriod(
        deduplicationCompletionResponse = duplicateResponse,
        defaultDuration = deduplicationDuration,
        skews = skews,
      )
      eventuallyAccepted <- succeedsEventually(
        maxRetryDuration = deduplicationDurationFromPeriod + skews + 10.seconds,
        description =
          s"Deduplication period expires and request is accepted for command ${submitRequest.getCommands}.",
        delayMechanism = delay,
      ) {
        submitAndAssertAccepted(thirdCall)
      }
      _ = assert(
        time.Duration
          .between(
            firstAcceptedCommand.getSynchronizerTime.getRecordTime.asJavaInstant,
            eventuallyAccepted.getSynchronizerTime.getRecordTime.asJavaInstant,
          )
          .toNanos > deduplicationDuration.toNanos,
        s"Interval between accepted commands is smaller than the deduplication duration. First accepted command record time: ${firstAcceptedCommand.getSynchronizerTime.getRecordTime.asJavaInstant}. Second accepted command record time: ${eventuallyAccepted.getSynchronizerTime.getRecordTime.asJavaInstant}",
      )
      _ <- submitAndAssertDeduplicated(
        fourthCall,
        LedgerString.assertFromString(eventuallyAccepted.submissionId),
        eventuallyAccepted.offset,
      )
      _ <- assertPartyHasActiveContracts(
        ledger,
        party = party,
        noOfActiveContracts = 2,
      )
    } yield {}
  }

  test(
    "DeduplicateSubmitterBasic",
    "Commands with identical submitter and command identifier should be deduplicated by the submission client",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val aliceRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger
      .submitRequest(bob, new Dummy(bob).create.commands)
      .update(_.commands.commandId := aliceRequest.getCommands.commandId)

    val aliceAcceptedSubmissionId = newSubmissionId()
    val bobAcceptedSubmissionId = newSubmissionId()

    for {
      // Submit a command as alice
      aliceResponse <- submitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(aliceRequest, aliceAcceptedSubmissionId),
        alice,
      )
      _ <- submitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(aliceRequest),
        aliceAcceptedSubmissionId,
        aliceResponse.offset,
        alice,
      )

      // Submit another command that uses same commandId, but is submitted by Bob
      bobResponse <- submitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(bobRequest, bobAcceptedSubmissionId),
        bob,
      )
      _ <- submitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(bobRequest),
        bobAcceptedSubmissionId,
        bobResponse.offset,
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
    "DeduplicateSubmitterCommandClient",
    "Commands with identical submitter and command identifier should be deduplicated by the command client",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val aliceRequest = ledger.submitAndWaitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger
      .submitAndWaitRequest(bob, new Dummy(bob).create.commands)
      .update(_.commands.commandId := aliceRequest.getCommands.commandId)

    val aliceAcceptedSubmissionId = newSubmissionId()
    val bobAcceptedSubmissionId = newSubmissionId()
    for {
      // Submit a command as alice
      aliceResponse <- submitAndWaitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(aliceRequest, aliceAcceptedSubmissionId),
        alice,
      )
      _ <- submitAndWaitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(aliceRequest),
        aliceAcceptedSubmissionId,
        aliceResponse.offset,
      )

      // Submit another command that uses same commandId, but is submitted by Bob
      bobReponse <- submitAndWaitRequestAndAssertCompletionAccepted(
        ledger,
        updateSubmissionId(bobRequest, bobAcceptedSubmissionId),
        bob,
      )
      _ <- submitAndWaitRequestAndAssertDeduplication(
        ledger,
        updateWithFreshSubmissionId(bobRequest),
        bobAcceptedSubmissionId,
        bobReponse.offset,
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
    shortIdentifier = s"DeduplicateUsingDurations",
    description = "Deduplicate commands within the deduplication period defined by a duration",
    partyAllocation = allocate(SingleParty),
    runConcurrently = false, // updates the time model
    disabledReason =
      "Most of the assertions run on async responses. Also, ledgers with the sync-only deduplication support use the wall clock for deduplication.",
  )(implicit ec =>
    _ => { case Participants(Participant(ledger, Seq(party))) =>
      val request = ledger
        .submitRequest(party, new DummyWithAnnotation(party, "Duplicate command").create.commands)
        .update(
          _.commands.deduplicationPeriod :=
            DeduplicationPeriod.DeduplicationDuration(deduplicationDuration.asProtobuf)
        )
      val firstAcceptedSubmissionId = newSubmissionId()
      runWithTimeModel { minMaxSkewSum =>
        for {
          completionResponse <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, firstAcceptedSubmissionId),
            party,
          )
          deduplicationCompletionResponse <- submitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(request),
            firstAcceptedSubmissionId,
            completionResponse.offset,
            party,
          )
          deduplicationDurationFromPeriod = extractDurationFromDeduplicationPeriod(
            deduplicationCompletionResponse = Some(deduplicationCompletionResponse),
            defaultDuration = deduplicationDuration,
            skews = minMaxSkewSum,
          )
          eventuallyAcceptedCompletionResponse <- succeedsEventually(
            maxRetryDuration = deduplicationDurationFromPeriod + minMaxSkewSum + 10.seconds,
            description =
              s"The deduplication period expires and the request is accepted for the commands ${request.getCommands}.",
            delayMechanism = ledger.delayMechanism,
          ) {
            submitRequestAndAssertCompletionAccepted(
              ledger,
              updateSubmissionId(request, firstAcceptedSubmissionId),
              party,
            )
          }
          _ <- assertPartyHasActiveContracts(
            ledger,
            party,
            noOfActiveContracts = 2,
          )
          _ <- assertDeduplicationDuration(
            deduplicationDuration.asProtobuf,
            deduplicationCompletionResponse,
            party,
            ledger,
          )
          _ <- assertDeduplicationDuration(
            deduplicationDuration.asProtobuf,
            eventuallyAcceptedCompletionResponse,
            party,
            ledger,
          )
        } yield {
          assert(
            completionResponse.commandId == request.commands.value.commandId,
            "The command ID of the first completion does not match the command ID of the submission",
          )
        }
      }
    }
  )

  testGivenAllParticipants(
    "DeduplicateUsingOffsets",
    "Deduplicate commands within the deduplication period defined by the offset",
    allocate(SingleParty),
    runConcurrently = false, // updates the time model
  )(implicit ec =>
    _ => { case Participants(Participant(ledger, Seq(party))) =>
      val request = ledger
        .submitRequest(party, new DummyWithAnnotation(party, "Duplicate command").create.commands)
      val acceptedSubmissionId = newSubmissionId()
      runWithTimeModel { _ =>
        val dummyRequest = ledger.submitRequest(
          party,
          new DummyWithAnnotation(
            party,
            "Dummy command to generate a completion offset",
          ).create.commands,
        )
        for {
          // Send a dummy command to the ledger so that we obtain a recent offset
          // We should be able to just grab the current ledger end,
          // but the converter from offsets to durations cannot handle this yet.
          offsetBeforeFirstCompletion <- submitRequestAndAssertCompletionAccepted(
            ledger,
            dummyRequest,
            party,
          ).map(_.offset)
          firstAcceptedResponse <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateSubmissionId(request, acceptedSubmissionId),
            party,
          )
          // Submit command again using the first offset as the deduplication offset
          duplicateResponse <- submitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(
              request.update(
                _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                  offsetBeforeFirstCompletion
                )
              )
            ),
            acceptedSubmissionId,
            firstAcceptedResponse.offset,
            party,
          )
          // Submit command again using the rejection offset as a deduplication period
          secondAcceptedResponse <- submitRequestAndAssertCompletionAccepted(
            ledger,
            updateWithFreshSubmissionId(
              request.update(
                _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                  duplicateResponse.offset
                )
              )
            ),
            party,
          )
        } yield {
          assertDeduplicationOffset(
            firstAcceptedResponse,
            duplicateResponse,
          )
          assertDeduplicationOffset(
            duplicateResponse,
            secondAcceptedResponse,
          )
        }
      }
    }
  )

  protected def assertPartyHasActiveContracts(
      ledger: ParticipantTestContext,
      party: Party,
      noOfActiveContracts: Int,
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger
      .activeContracts(Some(Seq(party)))
      .map(contracts =>
        assert(
          contracts.sizeIs == noOfActiveContracts,
          s"Expected $noOfActiveContracts active contracts for $party but found ${contracts.length} active contracts",
        )
      )

  protected def submitRequestAndAssertCompletionAccepted(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    submitRequestAndAssertCompletion(ledger, request, parties*) { completion =>
      assertCompletionStatus(request.toString, completion, Code.OK)
    }

  protected def submitAndWaitRequestAndAssertCompletionAccepted(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    submitAndWaitRequestAndAssertCompletion(ledger, request, parties*) { completion =>
      assertCompletionStatus(request.toString, completion, Code.OK)
    }

  private def submitRequestAndAssertSyncFailure(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      grpcCode: Code,
      errorCode: ErrorCode,
      additionalErrorAssertions: Throwable => Unit = _ => (),
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger
      .submit(request)
      .mustFail(s"Request expected to fail with code $grpcCode")
      .map(
        assertGrpcError(
          _,
          errorCode,
          exceptionMessageSubstring = None,
          checkDefiniteAnswerMetadata = true,
          additionalErrorAssertions,
        )
      )

  protected def submitAndWaitRequestAndAssertDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: Long,
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger
      .submitRequestAndTolerateGrpcError(
        ConsistencyErrors.SubmissionAlreadyInFlight,
        _.submitAndWait(request),
      )
      .mustFail("Request was accepted but we were expecting it to fail with a duplicate error")
      .map(
        assertGrpcError(
          _,
          ConsistencyErrors.DuplicateCommand,
          None,
          checkDefiniteAnswerMetadata = true,
          additionalErrorAssertions = assertDeduplicatedSubmissionIdAndOffsetOnError(
            acceptedSubmissionId,
            acceptedOffset,
            _,
          ),
        )
      )

  protected def submitRequestAndAssertDeduplication(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      acceptedSubmissionId: SubmissionId,
      acceptedOffset: Long,
      parties: Party*
  )(implicit ec: ExecutionContext): Future[Completion] =
    submitRequestAndAssertCompletion(
      ledger,
      request,
      parties*
    ) { completion =>
      assertCompletionStatus(
        request.toString,
        completion,
        Code.ALREADY_EXISTS, // Deduplication error
      )
      assertDeduplicatedSubmissionIdAndOffsetOnCompletion(
        acceptedSubmissionId,
        acceptedOffset,
        completion,
      )
    }

  private def assertCompletionStatus(
      requestString: String,
      response: Completion,
      statusCodes: Code*
  ): Unit =
    assert(
      statusCodes.exists(response.getStatus.code == _.value()),
      s"""Expecting completion with status code(s) ${statusCodes.mkString(
          ","
        )} but completion has status ${response.status}.
         |Request: $requestString
         |Response: $response
         |Metadata: ${extractErrorInfoMetadata(
          GrpcStatus.toJavaProto(response.getStatus)
        )}""".stripMargin,
    )

  private def assertDeduplicatedSubmissionIdAndOffsetOnError(
      acceptedSubmissionId: SubmissionId,
      acceptedCompletionOffset: Long,
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
      acceptedCompletionOffset: Long,
      response: Completion,
  ): Unit = {
    val metadata = extractErrorInfoMetadata(
      GrpcStatus.toJavaProto(response.getStatus)
    )
    assertExistingSubmissionIdOnMetadata(metadata, acceptedSubmissionId)
    assertExistingCompletionOffsetOnMetadata(metadata, acceptedCompletionOffset)
  }

  private def assertExistingSubmissionIdOnMetadata(
      metadata: Map[String, String],
      acceptedSubmissionId: SubmissionId,
  ): Unit =
    metadata.get("existing_submission_id").foreach { metadataExistingSubmissionId =>
      assertEquals(
        "submission ID mismatch",
        metadataExistingSubmissionId,
        acceptedSubmissionId,
      )
    }

  private def assertExistingCompletionOffsetOnMetadata(
      metadata: Map[String, String],
      acceptedCompletionOffset: Long,
  ): Unit =
    metadata.get("completion_offset").foreach { offset =>
      assertEquals(
        "completion offset mismatch",
        offset.toLong,
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
  ): Future[Completion] =
    ledger
      .submitRequestAndTolerateGrpcError(
        ConsistencyErrors.SubmissionAlreadyInFlight,
        submitRequestAndFindCompletion(_, request, parties*),
      )
      .map { response =>
        additionalCompletionAssertion(response)
        response
      }

  private def submitAndWaitRequestAndAssertCompletion(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(
      additionalCompletionAssertion: Completion => Unit
  )(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    submitRequestAndFindCompletion(ledger, request, parties*).map { response =>
      additionalCompletionAssertion(response)
      response
    }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    submitRequest(ledger)(request)
      .flatMap { ledgerEnd =>
        ledger
          .findCompletion(ledger.completionStreamRequest(ledgerEnd)(parties*)) { completion =>
            request.commands.map(_.submissionId).contains(completion.submissionId)
          }
          .map(_.toList)
      }
      .map { completions =>
        assertSingleton("Expected only one completion", completions)
      }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[Completion] =
    ledger
      .submitAndWait(request)
      .flatMap { _ =>
        ledger
          .findCompletion(ledger.completionStreamRequest()(parties*)) { completion =>
            request.commands.map(_.submissionId).contains(completion.submissionId)
          }
          .map(_.toList)
      }
      .map { completions =>
        assert(
          completions.headOption.value.offset > 0,
          "Expected a populated completion offset",
        )
        assertSingleton("Expected only one completion", completions)
      }

  protected def submitRequest(
      ledger: ParticipantTestContext
  )(
      request: SubmitRequest
  )(implicit ec: ExecutionContext): Future[Long] = for {
    ledgerEnd <- ledger.currentEnd()
    _ <- ledger.submit(request)
  } yield {
    ledgerEnd
  }

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

  val defaultCantonSkew = 365.days

  private def runWithTimeModel(
      testWithDelayMechanism: FiniteDuration => Future[Unit]
  ): Future[Unit] =
    testWithDelayMechanism(Durations.asFiniteDuration(defaultCantonSkew))

  private def extractDurationFromDeduplicationPeriod(
      deduplicationCompletionResponse: Option[Completion],
      defaultDuration: FiniteDuration,
      skews: FiniteDuration,
  ): FiniteDuration =
    deduplicationCompletionResponse
      .map(_.deduplicationPeriod)
      .map {
        case CompletionDeduplicationPeriod.Empty =>
          throw new IllegalStateException("received empty completion")
        case CompletionDeduplicationPeriod.DeduplicationOffset(_) =>
          defaultDuration
        case CompletionDeduplicationPeriod.DeduplicationDuration(value) =>
          value.asScala match {
            case infinite: Duration.Infinite =>
              sys.error("infinite deduplication duration not supported")
            case duration: FiniteDuration => duration
          }
      }
      .getOrElse(defaultDuration + skews)
}
