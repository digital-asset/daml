// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.daml.api.util.TimeProvider
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
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  TrackedCompletionFailure,
}
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
import com.daml.platform.server.api.ApiException
import com.daml.platform.server.api.services.grpc.GrpcCommandService
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import io.grpc.Status
import scalaz.syntax.tag._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
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

  private def submitAndWaitInternal(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContext
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    val commands = request.getCommands
    withEnrichedLoggingContext(
      logging.commandId(commands.commandId),
      logging.partyString(commands.party),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { implicit loggingContext =>
      if (running) {
        submissionTracker.track(CommandSubmission(commands))
      } else {
        Future.failed(
          new ApiException(Status.UNAVAILABLE.withDescription("Service has been shut down."))
        )
      }.andThen(logger.logErrorsOnCall[Completion])
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
}

private[apiserver] object ApiCommandService {

  private val trackerCleanupInterval = 30.seconds

  type SubmissionFlow = Flow[
    Ctx[(Promise[Either[CompletionFailure, CompletionSuccess]], String), CommandSubmission],
    Ctx[(Promise[Either[CompletionFailure, CompletionSuccess]], String), Try[Empty]],
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
        ledgerConfigurationSubscription,
        metrics,
      ),
      trackerCleanupInterval,
    )
    new GrpcCommandService(
      service = new ApiCommandService(transactionServices, submissionTracker),
      ledgerId = configuration.ledgerId,
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
      trackerRetentionPeriod: FiniteDuration,
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
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
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
      // Use just name of first party for open-ended metrics to avoid unbounded metrics name for multiple parties
      val metricsPrefixFirstParty = key.parties.min
      for {
        ledgerEnd <- completionServices.getCompletionEnd().map(_.getOffset)
      } yield {
        val commandTrackerFlow =
          CommandTrackerFlow[Promise[Either[CompletionFailure, CompletionSuccess]], NotUsed](
            submissionFlow,
            offset =>
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
            ledgerEnd,
            () => ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationTime),
          )
        val trackingFlow = MaxInFlight(
          configuration.maxCommandsInFlight,
          capacityCounter = metrics.daml.commands.maxInFlightCapacity(metricsPrefixFirstParty),
          lengthCounter = metrics.daml.commands.maxInFlightLength(metricsPrefixFirstParty),
        ).joinMat(commandTrackerFlow)(Keep.right)

        QueueBackedTracker(
          trackingFlow,
          configuration.inputBufferSize,
          capacityCounter = metrics.daml.commands.inputBufferCapacity(metricsPrefixFirstParty),
          lengthCounter = metrics.daml.commands.inputBufferLength(metricsPrefixFirstParty),
          delayTimer = metrics.daml.commands.inputBufferDelay(metricsPrefixFirstParty),
        )
      }
    }
  }

}
