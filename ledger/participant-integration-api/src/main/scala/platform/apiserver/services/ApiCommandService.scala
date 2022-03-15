// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.daml.api.util.TimeProvider
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse,
}
import com.daml.ledger.api.validation.{CommandsValidator, LedgerOffsetValidator}
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  TrackedCompletionFailure,
}
import com.daml.ledger.client.services.commands.tracker.{CompletionResponse, TrackedCommandKey}
import com.daml.ledger.client.services.commands.{
  CommandCompletionSource,
  CommandSubmission,
  CommandTrackerFlow,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.services.ApiCommandService._
import com.daml.platform.apiserver.services.tracking.{QueueBackedTracker, Tracker, TrackerMap}
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.platform.server.api.services.grpc.GrpcCommandService
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import io.grpc.Context

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

private[apiserver] final class ApiCommandService private[services] (
    transactionServices: TransactionServices,
    submissionTracker: Tracker,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandServiceGrpc.CommandService
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  @volatile private var running = true

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    running = false
    submissionTracker.close()
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).map {
        case Left(failure) =>
          throw CompletionResponse.toException(failure)(errorLogger)
        case Right(_) => Empty.defaultInstance
      }
    }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).map {
        case Left(failure) =>
          throw CompletionResponse.toException(failure)(errorLogger)
        case Right(response) =>
          SubmitAndWaitForTransactionIdResponse.of(
            response.transactionId,
            offsetFromResponse(response),
          )
      }
    }

  private def offsetFromResponse(response: CompletionSuccess) =
    response.checkpoint.flatMap(_.offset).flatMap(_.value.absolute).getOrElse("")

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).flatMap {
        case Left(failure) =>
          Future.failed(CompletionResponse.toException(failure)(errorLogger))
        case Right(resp) =>
          val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
          val txRequest = GetTransactionByIdRequest(
            request.getCommands.ledgerId,
            resp.transactionId,
            effectiveActAs.toList,
          )
          transactionServices
            .getFlatTransactionById(txRequest)
            .map(transactionResponse =>
              SubmitAndWaitForTransactionResponse
                .of(
                  transactionResponse.transaction,
                  transactionResponse.transaction.map(_.offset).getOrElse(""),
                )
            )
      }
    }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).flatMap {
        case Left(failure) =>
          Future.failed(CompletionResponse.toException(failure)(errorLogger))
        case Right(resp) =>
          val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
          val txRequest = GetTransactionByIdRequest(
            request.getCommands.ledgerId,
            resp.transactionId,
            effectiveActAs.toList,
          )
          transactionServices
            .getTransactionById(txRequest)
            .map(resp =>
              SubmitAndWaitForTransactionTreeResponse
                .of(resp.transaction, resp.transaction.map(_.offset).getOrElse(""))
            )
      }
    }

  private def submitAndWaitInternal(
      request: SubmitAndWaitRequest
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
    if (running) {
      val timeout = Option(Context.current().getDeadline)
        .map(deadline => Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS)))
      submissionTracker.track(CommandSubmission(request.getCommands, timeout))
    } else {
      Future
        .failed(
          LedgerApiErrors.ServiceNotRunning.Reject("Command Service").asGrpcError
        )
    }

  private def withCommandsLoggingContext[T](commands: Commands)(
      submitWithContext: (LoggingContext, ContextualizedErrorLogger) => Future[T]
  )(implicit loggingContext: LoggingContext): Future[T] =
    withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.partyString(commands.party),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { loggingContext =>
      submitWithContext(
        loggingContext,
        new DamlContextualizedErrorLogger(
          logger,
          loggingContext,
          Some(commands.submissionId),
        ),
      )
    }
}

private[apiserver] object ApiCommandService {

  private val trackerCleanupInterval = 30.seconds

  type SubmissionFlow = Flow[
    Ctx[
      (Promise[Either[CompletionFailure, CompletionSuccess]], TrackedCommandKey),
      CommandSubmission,
    ],
    Ctx[(Promise[Either[CompletionFailure, CompletionSuccess]], TrackedCommandKey), Try[Empty]],
    NotUsed,
  ]

  def create(
      configuration: Configuration,
      submissionFlow: SubmissionFlow,
      completionServices: CommandCompletionService,
      transactionServices: TransactionServices,
      timeProvider: TimeProvider,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): CommandServiceGrpc.CommandService with GrpcApiService = {
    val submissionTracker = new TrackerMap.SelfCleaning(
      configuration.trackerRetentionPeriod,
      Tracking.getTrackerKey,
      Tracking.newTracker(
        configuration,
        submissionFlow,
        completionServices,
        metrics,
      ),
      trackerCleanupInterval,
    )
    new GrpcCommandService(
      service = new ApiCommandService(transactionServices, submissionTracker),
      ledgerId = configuration.ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationDuration = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
      generateSubmissionId = SubmissionIdGenerator.Random,
    )
  }

  final case class Configuration(
      ledgerId: LedgerId,
      inputBufferSize: Int,
      maxCommandsInFlight: Int,
      trackerRetentionPeriod: Duration,
  )

  final class TransactionServices(
      val getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
      val getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse],
  )

  private object Tracking {
    final case class Key(applicationId: Ref.ApplicationId, parties: Set[Ref.Party])

    private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

    def getTrackerKey(commands: Commands): Tracking.Key = {
      val parties = CommandsValidator.effectiveActAs(commands)
      // Safe to convert the applicationId and the parties as the command should've been already validated
      Tracking.Key(
        Ref.ApplicationId.assertFromString(commands.applicationId),
        parties.map(Ref.Party.assertFromString),
      )
    }

    def newTracker(
        configuration: Configuration,
        submissionFlow: SubmissionFlow,
        completionServices: CommandCompletionService,
        metrics: Metrics,
    )(
        key: Tracking.Key
    )(implicit
        materializer: Materializer,
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Tracker] = {
      // Note: command completions are returned as long as at least one of the original submitters
      // is specified in the command completion request.
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)
      for {
        ledgerEnd <- completionServices.getLedgerEnd()
      } yield {
        val commandTrackerFlow =
          CommandTrackerFlow[Promise[Either[CompletionFailure, CompletionSuccess]], NotUsed](
            commandSubmissionFlow = submissionFlow,
            createCommandCompletionSource = offset =>
              LedgerOffsetValidator
                .validate(offset, "command_tracker_offset")
                .fold(
                  Source.failed,
                  offset =>
                    completionServices
                      .completionStreamSource(
                        CompletionStreamRequest(
                          Some(configuration.ledgerId),
                          key.applicationId,
                          key.parties,
                          Some(offset),
                        )
                      )
                      .mapConcat(CommandCompletionSource.toStreamElements),
                ),
            startingOffset = LedgerOffset(LedgerOffset.Value.Absolute(ledgerEnd.value)),
            // We use the tracker retention period as the maximum command timeout, because the is
            // the maximum length of time we can guarantee that the tracker stays alive (assuming
            // the server stays up). If we set it any longer, the tracker might be shut down while
            // we wait.
            // In the future, we may want to configure this separately, but for now, we use re-use
            // the tracker retention period for convenience.
            maximumCommandTimeout = configuration.trackerRetentionPeriod,
          )
        val trackingFlow = MaxInFlight(
          configuration.maxCommandsInFlight,
          capacityCounter = metrics.daml.commands.maxInFlightCapacity,
          lengthCounter = metrics.daml.commands.maxInFlightLength,
        ).joinMat(commandTrackerFlow)(Keep.right)

        QueueBackedTracker(
          trackingFlow,
          configuration.inputBufferSize,
          capacityCounter = metrics.daml.commands.inputBufferCapacity,
          lengthCounter = metrics.daml.commands.inputBufferLength,
          delayTimer = metrics.daml.commands.inputBufferDelay,
        )
      }
    }
  }
}
