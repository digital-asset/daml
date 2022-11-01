// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.time
import com.daml.error.ErrorCode
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.FutureAssertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.assertions.CommandDeduplicationAssertions.{assertDeduplicationDuration, assertDeduplicationOffset}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.CompletionResponse
import com.daml.ledger.api.testtool.infrastructure.time.{DelayMechanism, Durations}
import com.daml.ledger.api.v1.admin.config_management_service.TimeModel
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion.{DeduplicationPeriod => CompletionDeduplicationPeriod}
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.OffsetSupport
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.DA.Types.Tuple2
import com.daml.ledger.test.model.Test.{Dummy, DummyWithAnnotation, TextKey, TextKeyOperations}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{LedgerString, SubmissionId}
import com.daml.logging.LoggingContext
import com.daml.platform.error.definitions.LedgerApiErrors
import io.grpc.Status.Code
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final class CommandDeduplicationIT(
    timeoutScaleFactor: Double
) extends LedgerTestSuite {

  private[this] val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val deduplicationDuration: FiniteDuration =
    Durations.scaleDuration(2.seconds, timeoutScaleFactor)

  test(
    s"SimpleDeduplicationBasic",
    "Deduplicate commands within the deduplication duration window",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger
      .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
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
        response.completion.commandId == request.commands.get.commandId,
        "The command ID of the first completion does not match the command ID of the submission",
      )
    }
  })

  test(
    s"StopOnSubmissionFailure",
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
    s"StopOnCompletionFailure",
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
        ko.exerciseTKOFetchAndRecreate(Tuple2(party, key)).command,
      )
      requestB = ledger.submitAndWaitRequest(
        party,
        ko.exerciseTKOFetchAndRecreate(Tuple2(party, key)).command,
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

  test(
    s"SimpleDeduplicationCommandClient",
    "Deduplicate commands within the deduplication time window using the command client",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(
        _.commands.deduplicationTime := deduplicationDuration.asProtobuf
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
    configuredParticipants => { case Participants(Participant(ledger, parties @ _*)) =>
      def generateVariations(elements: List[List[Boolean]]): List[List[Boolean]] =
        elements match {
          case Nil => List(Nil)
          case currentElement :: tail =>
            currentElement.flatMap(value => generateVariations(tail).map(value :: _))
        }

      runWithTimeModel(configuredParticipants) { minMaxSkewSum =>
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
        submitAndWait: Boolean
    ): Future[CompletionResponse] = {
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
        acceptedLedgerOffset: LedgerOffset,
    ): Future[Option[CompletionResponse]] =
      if (submitAndWait)
        submitAndWaitRequestAndAssertDeduplication(
          ledger,
          updateWithFreshSubmissionId(submitAndWaitRequest),
          acceptedSubmissionId,
          acceptedLedgerOffset,
        ).map(_ => None)
      else
        submitRequestAndAssertDeduplication(
          ledger,
          updateWithFreshSubmissionId(submitRequest),
          acceptedSubmissionId,
          acceptedLedgerOffset,
          party,
        ).map(Some(_))

    for {
      // Submit command (first deduplication window)
      firstAcceptedCommand <- submitAndAssertAccepted(firstCall)
      duplicateResponse <- submitAndAssertDeduplicated(
        secondCall,
        LedgerString.assertFromString(firstAcceptedCommand.completion.submissionId),
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
          .between(firstAcceptedCommand.recordTime, eventuallyAccepted.recordTime)
          .toNanos > deduplicationDuration.toNanos,
        s"Interval between accepted commands is smaller than the deduplication duration. First accepted command record time: ${firstAcceptedCommand.recordTime}. Second accepted command record time: ${eventuallyAccepted.recordTime}",
      )
      _ <- submitAndAssertDeduplicated(
        fourthCall,
        LedgerString.assertFromString(eventuallyAccepted.completion.submissionId),
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
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger
      .submitRequest(bob, Dummy(bob).create.command)
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
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger
      .submitAndWaitRequest(bob, Dummy(bob).create.command)
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
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
        .update(
          _.commands.deduplicationPeriod :=
            DeduplicationPeriod.DeduplicationDuration(deduplicationDuration.asProtobuf)
        )
      val firstAcceptedSubmissionId = newSubmissionId()
      runWithTimeModel(configuredParticipants) { minMaxSkewSum =>
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
            completionResponse.completion.commandId == request.commands.get.commandId,
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
    enabled =
      !_.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport.isOffsetNotSupported,
    disabledReason = "Deduplication periods represented by offsets are not supported",
    runConcurrently = false, // updates the time model
  )(implicit ec =>
    configuredParticipants => { case Participants(Participant(ledger, party)) =>
      val request = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Duplicate command").create.command)
      val acceptedSubmissionId = newSubmissionId()
      runWithTimeModel(configuredParticipants) { _ =>
        val dummyRequest = ledger.submitRequest(
          party,
          DummyWithAnnotation(party, "Dummy command to generate a completion offset").create.command,
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
          // Wait for any ledgers that might adjust based on time skews
          // This is done so that we can validate later that the command is accepted again
          _ <- delayForOffsetIfRequired(ledger)
          // Submit command again using the first offset as the deduplication offset
          duplicateResponse <- submitRequestAndAssertDeduplication(
            ledger,
            updateWithFreshSubmissionId(
              request.update(
                _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
                  Ref.HexString.assertFromString(offsetBeforeFirstCompletion.getAbsolute)
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
                  Ref.HexString.assertFromString(duplicateResponse.offset.getAbsolute)
                )
              )
            ),
            party,
          )
        } yield {
          assertDeduplicationOffset(
            firstAcceptedResponse,
            duplicateResponse,
            ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport,
          )
          assertDeduplicationOffset(
            duplicateResponse,
            secondAcceptedResponse,
            ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport,
          )
        }
      }
    }
  )

  private def delayForOffsetIfRequired(
      ledger: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger.features.commandDeduplicationFeatures.getDeduplicationPeriodSupport.offsetSupport match {
      case OffsetSupport.OFFSET_NATIVE_SUPPORT =>
        Future.unit
      case OffsetSupport.OFFSET_CONVERT_TO_DURATION =>
        // the converted duration is calculated as the interval between submission time
        // and offset record time + minSkew (used to determine maxRecordTime)
        //
        // the duration is extended with up to minSkew + maxSkew when using pre-execution,
        // as we use maxRecordTime and minRecordTime to calculate the interval between the two commands
        //
        // thus, we delay by twice the skews below
        ledger
          .getTimeModel()
          .flatMap(response => {
            ledger.delayMechanism.delayBy(
              // skews are already scaled by the time factor
              Durations.asFiniteDuration(
                2 * (response.getTimeModel.getMaxSkew.asScala +
                  response.getTimeModel.getMinSkew.asScala)
              )
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
  ): Future[CompletionResponse] =
    submitRequestAndAssertCompletion(ledger, request, parties: _*) { completion =>
      assertCompletionStatus(request.toString, completion, Code.OK)
    }

  protected def submitAndWaitRequestAndAssertCompletionAccepted(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[CompletionResponse] =
    submitAndWaitRequestAndAssertCompletion(ledger, request, parties: _*) { completion =>
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
      acceptedOffset: LedgerOffset,
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger
      .submitAndWaitForTransaction(request)
      .mustFail("Request was accepted but we were expecting it to fail with a duplicate error")
      .map(
        assertGrpcError(
          _,
          errorCode = LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
          exceptionMessageSubstring = None,
          checkDefiniteAnswerMetadata = true,
          assertDeduplicatedSubmissionIdAndOffsetOnError(
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
      acceptedOffset: LedgerOffset,
      parties: Party*
  )(implicit ec: ExecutionContext): Future[CompletionResponse] =
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
      response: CompletionResponse,
      statusCode: Code,
  ): Unit =
    assert(
      response.completion.getStatus.code == statusCode.value(),
      s"""Expecting completion with status code $statusCode but completion has status ${response.completion.status}.
         |Request: $requestString
         |Response: $response
         |Metadata: ${extractErrorInfoMetadata(
          GrpcStatus.toJavaProto(response.completion.getStatus)
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
      response: CompletionResponse,
  ): Unit = {
    val metadata = extractErrorInfoMetadata(
      GrpcStatus.toJavaProto(response.completion.getStatus)
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
      acceptedCompletionOffset: LedgerOffset,
  ): Unit =
    metadata.get("completion_offset").foreach { offset =>
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
      additionalCompletionAssertion: CompletionResponse => Unit
  )(implicit
      ec: ExecutionContext
  ): Future[CompletionResponse] =
    submitRequestAndFindCompletion(ledger, request, parties: _*).map { response =>
      additionalCompletionAssertion(response)
      response
    }

  private def submitAndWaitRequestAndAssertCompletion(
      ledger: ParticipantTestContext,
      request: SubmitAndWaitRequest,
      parties: Party*
  )(
      additionalCompletionAssertion: CompletionResponse => Unit
  )(implicit
      ec: ExecutionContext
  ): Future[CompletionResponse] =
    submitRequestAndFindCompletion(ledger, request, parties: _*).map { response =>
      additionalCompletionAssertion(response)
      response
    }

  protected def submitRequestAndFindCompletion(
      ledger: ParticipantTestContext,
      request: SubmitRequest,
      parties: Party*
  )(implicit
      ec: ExecutionContext
  ): Future[CompletionResponse] =
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
  ): Future[CompletionResponse] =
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
        assert(
          completions.head.offset.getAbsolute.nonEmpty,
          "Expected a populated completion offset",
        )
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

  private def runWithTimeModel(participants: Seq[ParticipantTestContext])(
      testWithDelayMechanism: FiniteDuration => Future[Unit]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    // deduplication duration is adjusted by min skew and max skew when running using pre-execution
    // to account for this we adjust the time model
    val skew = Durations.scaleDuration(3.second, timeoutScaleFactor).asProtobuf
    runWithUpdatedTimeModel(
      participants,
      _.update(_.minSkew := skew, _.maxSkew := skew),
    ) { timeModel =>
      val minMaxSkewSum =
        Durations.asFiniteDuration(timeModel.getMinSkew.asScala + timeModel.getMaxSkew.asScala)
      testWithDelayMechanism(minMaxSkewSum)
    }
  }

  private def runWithUpdatedTimeModel(
      participants: Seq[ParticipantTestContext],
      timeModelUpdate: TimeModel => TimeModel,
  )(test: TimeModel => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    val anyParticipant = participants.head
    anyParticipant
      .getTimeModel()
      .flatMap(timeModel => {
        def restoreTimeModel(participant: ParticipantTestContext) = {
          val ledgerTimeModelRestoreResult = for {
            time <- participant.time()
            // retrieve current configuration generation, which can be updated by the test
            currentConfigurationGeneration <- participant
              .getTimeModel()
              .map(_.configurationGeneration)
            _ <- participant
              .setTimeModel(
                time.plusSeconds(1),
                currentConfigurationGeneration,
                timeModel.getTimeModel,
              )
          } yield {}
          ledgerTimeModelRestoreResult.recover { case NonFatal(exception) =>
            logger.warn("Failed to restore time model for ledger", exception)
            ()
          }
        }

        for {
          time <- anyParticipant.time()
          updatedModel = timeModelUpdate(timeModel.getTimeModel)
          (timeModelForTest, participantThatDidTheUpdate) <- tryTimeModelUpdateOnAllParticipants(
            participants,
            _.setTimeModel(
              time.plusSeconds(1),
              timeModel.configurationGeneration,
              updatedModel,
            ).map(_ => updatedModel),
          ).recover { case NonFatal(exception) =>
            logger.warn(
              "Failed to update time model for test. WIll run test with already configured time model",
              exception,
            )
            timeModel.getTimeModel -> anyParticipant
          }
          _ <- test(timeModelForTest)
            .transformWith(testResult =>
              restoreTimeModel(participantThatDidTheUpdate).transform(_ => testResult)
            )
        } yield {}
      })
  }

  /** Try to run the update sequentially on all the participants.
    * The function returns the first success or the last failure of the update operation.
    * Useful for updating the configuration when we don't know which participant can update the config,
    * as only the first one that submitted the initial configuration has the permissions to do so.
    */
  private def tryTimeModelUpdateOnAllParticipants(
      participants: Seq[ParticipantTestContext],
      timeModelUpdate: ParticipantTestContext => Future[TimeModel],
  )(implicit ec: ExecutionContext): Future[(TimeModel, ParticipantTestContext)] = {
    participants.foldLeft(
      Future.failed[(TimeModel, ParticipantTestContext)](
        new IllegalStateException("No participant")
      )
    ) { (result, participant) =>
      result.recoverWith { case NonFatal(_) =>
        timeModelUpdate(participant).map(_ -> participant)
      }
    }
  }

  private def extractDurationFromDeduplicationPeriod(
      deduplicationCompletionResponse: Option[CompletionResponse],
      defaultDuration: FiniteDuration,
      skews: FiniteDuration,
  ): FiniteDuration =
    deduplicationCompletionResponse
      .map(_.completion.deduplicationPeriod)
      .map {
        case CompletionDeduplicationPeriod.Empty =>
          throw new IllegalStateException("received empty completion")
        case CompletionDeduplicationPeriod.DeduplicationOffset(_) =>
          defaultDuration
        case CompletionDeduplicationPeriod.DeduplicationDuration(value) =>
          value.asScala
      }
      .getOrElse(defaultDuration + skews)
      .asInstanceOf[FiniteDuration]

}
