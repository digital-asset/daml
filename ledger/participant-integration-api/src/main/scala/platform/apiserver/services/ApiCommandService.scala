// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.daml.api.util.TimeProvider
import com.daml.error.{DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndResponse,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse,
}
import com.daml.ledger.api.validation.CommandsValidator
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
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.services.ApiCommandService._
import com.daml.platform.apiserver.services.tracking.{QueueBackedTracker, Tracker, TrackerMap}
import com.daml.platform.server.api.services.grpc.GrpcCommandService
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import io.grpc.Context
import scalaz.syntax.tag._

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

private[apiserver] final class ApiCommandService private[services] (
    transactionServices: TransactionServices,
    submissionTracker: Tracker,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandServiceGrpc.CommandService
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)

  @volatile private var running = true

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    running = false
    submissionTracker.close()
  }

  private def submitAndWaitInternal(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContext
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    val commands = request.getCommands
    withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.partyString(commands.party),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { implicit loggingContext =>
      if (running) {
        val timeout = Option(Context.current().getDeadline)
          .map(deadline => Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS)))
        submissionTracker.track(CommandSubmission(commands, timeout))
      } else {
        handleFailure(request, loggingContext)
      }
    }
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    submitAndWaitInternal(request).map(
      _.fold(
        failure => throw CompletionResponse.toException(failure),
        _ => Empty.defaultInstance,
      )
    )

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    submitAndWaitInternal(request).map(
      _.fold(
        failure => throw CompletionResponse.toException(failure),
        response => SubmitAndWaitForTransactionIdResponse(response.transactionId),
      )
    )

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    submitAndWaitInternal(request).flatMap { resp =>
      resp.fold(
        failure => Future.failed(CompletionResponse.toException(failure)),
        { resp =>
          val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
          val txRequest = GetTransactionByIdRequest(
            request.getCommands.ledgerId,
            resp.transactionId,
            effectiveActAs.toList,
          )
          transactionServices
            .getFlatTransactionById(txRequest)
            .map(resp => SubmitAndWaitForTransactionResponse(resp.transaction))
        },
      )
    }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    submitAndWaitInternal(request).flatMap { resp =>
      resp.fold(
        failure => Future.failed(CompletionResponse.toException(failure)),
        { resp =>
          val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
          val txRequest = GetTransactionByIdRequest(
            request.getCommands.ledgerId,
            resp.transactionId,
            effectiveActAs.toList,
          )
          transactionServices
            .getTransactionById(txRequest)
            .map(resp => SubmitAndWaitForTransactionTreeResponse(resp.transaction))
        },
      )
    }

  private def handleFailure(
      request: SubmitAndWaitRequest,
      loggingContext: LoggingContext,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
    Future
      .failed(
        errorFactories.serviceNotRunning("Commands Service")(definiteAnswer = Some(false))(
          new DamlContextualizedErrorLogger(
            logger,
            loggingContext,
            request.commands.map(_.submissionId),
          )
        )
      )
      .andThen(logger.logErrorsOnCall[Completion](loggingContext))
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
      completionServices: CompletionServices,
      transactionServices: TransactionServices,
      timeProvider: TimeProvider,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      metrics: Metrics,
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
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
        ErrorFactories(errorCodesVersionSwitcher),
      ),
      trackerCleanupInterval,
    )
    new GrpcCommandService(
      service =
        new ApiCommandService(transactionServices, submissionTracker, errorCodesVersionSwitcher),
      ledgerId = configuration.ledgerId,
      errorCodesVersionSwitcher = errorCodesVersionSwitcher,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationTime),
      generateSubmissionId = SubmissionIdGenerator.Random,
    )
  }

  final case class Configuration(
      ledgerId: LedgerId,
      inputBufferSize: Int,
      maxCommandsInFlight: Int,
      trackerRetentionPeriod: Duration,
  )

  final class CompletionServices(
      val getCompletionSource: CompletionStreamRequest => Source[CompletionStreamResponse, NotUsed],
      val getCompletionEnd: () => Future[CompletionEndResponse],
  )

  final class TransactionServices(
      val getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
      val getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse],
  )

  private object Tracking {
    final case class Key(applicationId: String, parties: Set[String])

    def getTrackerKey(commands: Commands): Tracking.Key = {
      val parties = CommandsValidator.effectiveActAs(commands)
      Tracking.Key(commands.applicationId, parties)
    }

    def newTracker(
        configuration: Configuration,
        submissionFlow: SubmissionFlow,
        completionServices: CompletionServices,
        metrics: Metrics,
        errorFactories: ErrorFactories,
    )(
        key: Tracking.Key
    )(implicit
        materializer: Materializer,
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Tracker] = {
      // Note: command completions are returned as long as at least one of the original submitters
      // is specified in the command completion request.
      for {
        ledgerEnd <- completionServices.getCompletionEnd().map(_.getOffset)
      } yield {
        val commandTrackerFlow =
          CommandTrackerFlow[Promise[Either[CompletionFailure, CompletionSuccess]], NotUsed](
            commandSubmissionFlow = submissionFlow,
            createCommandCompletionSource = offset =>
              completionServices
                .getCompletionSource(
                  CompletionStreamRequest(
                    configuration.ledgerId.unwrap,
                    key.applicationId,
                    key.parties.toList,
                    Some(offset),
                  )
                )
                .mapConcat(CommandCompletionSource.toStreamElements),
            startingOffset = ledgerEnd,
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
          errorFactories = errorFactories,
        )
      }
    }
  }

}
