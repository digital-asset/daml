// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.daml.api.util.TimeProvider
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.{ApplicationId, Commands, LedgerId, SubmissionId}
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse,
}
import com.daml.ledger.api.validation.{
  CommandsValidator,
  LedgerOffsetValidator,
  SubmitAndWaitRequestValidator,
}
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.client.services.commands.tracker.{CompletionResponse, TrackedCommandKey}
import com.daml.ledger.client.services.commands.{CommandCompletionSource, CommandTrackerFlow}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.services.ApiCommandService._
import com.daml.platform.apiserver.services.tracking.{
  InternalCommandSubmission,
  QueueBackedTracker,
  Tracker,
  TrackerMap,
}
import com.daml.platform.server.api.services.domain.{CommandCompletionService, CommandService}
import com.daml.platform.server.api.services.grpc.GrpcCommandService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

private[apiserver] final class ApiCommandService private[services] (
    transactionServices: TransactionServices,
    submissionTracker: Tracker[InternalCommandSubmission],
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandService
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)

  @volatile private var running = true

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    running = false
    submissionTracker.close()
  }

  override def submitAndWait(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    submitAndWaitInternal(request, trackingTimeout).map(response =>
      SubmitAndWaitForTransactionIdResponse.of(
        response.transactionId,
        offsetFromResponse(response),
      )
    )

  private def offsetFromResponse(response: CompletionSuccess) =
    response.checkpoint.flatMap(_.offset).flatMap(_.value.absolute).getOrElse("")

  override def submitAndWaitForTransaction(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionResponse] =
    submitAndWaitInternal(request, trackingTimeout).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.commands.ledgerId.unwrap,
        resp.transactionId,
        request.commands.actAs.toSeq,
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

  override def submitAndWaitForTransactionTree(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    submitAndWaitInternal(request, trackingTimeout).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.commands.ledgerId.unwrap,
        resp.transactionId,
        request.commands.actAs.toList,
      )
      transactionServices
        .getTransactionById(txRequest)
        .map(resp =>
          SubmitAndWaitForTransactionTreeResponse
            .of(resp.transaction, resp.transaction.map(_.offset).getOrElse(""))
        )
    }

  private def submitAndWaitInternal(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  )(implicit
      loggingContext: LoggingContext
  ): Future[CompletionSuccess] = {
    implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(
        logger,
        loggingContext,
        request.commands.submissionId.map(SubmissionId.unwrap),
      )
    if (running) {
      submissionTracker
        .track(InternalCommandSubmission(request, trackingTimeout))
        .map(
          _.fold(
            failure => {
              val contextualizedErrorLogger: ContextualizedErrorLogger =
                new DamlContextualizedErrorLogger(
                  logger,
                  loggingContext,
                  request.commands.submissionId.map(SubmissionId.unwrap),
                )
              throw CompletionResponse.toException(failure, errorFactories)(
                contextualizedErrorLogger
              )
            },
            identity,
          )
        )
    } else {
      Future
        .failed(
          errorFactories.serviceNotRunning("Command Service")(definiteAnswer = Some(false))
        )
    }
  }
}

private[apiserver] object ApiCommandService {

  private val trackerCleanupInterval = 30.seconds

  type SubmissionFlow = Flow[
    Ctx[
      (Promise[Either[CompletionFailure, CompletionSuccess]], TrackedCommandKey),
      InternalCommandSubmission,
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
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): CommandServiceGrpc.CommandService with GrpcApiService = {
    val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
    val ledgerOffsetValidator = new LedgerOffsetValidator(errorFactories)
    val submissionTracker = new TrackerMap.SelfCleaning(
      configuration.trackerRetentionPeriod,
      (internalSubmission: InternalCommandSubmission) =>
        Tracking.getTrackerKey(internalSubmission.request.commands),
      Tracking.newTracker(
        configuration,
        submissionFlow,
        completionServices,
        metrics,
        errorFactories,
        ledgerOffsetValidator,
      ),
      trackerCleanupInterval,
    )
    val commandsValidator = new CommandsValidator(
      configuration.ledgerId,
      errorCodesVersionSwitcher,
    )
    val fieldValidations = FieldValidations(ErrorFactories(errorCodesVersionSwitcher))
    val validator = new SubmitAndWaitRequestValidator(commandsValidator, fieldValidations)
    new GrpcCommandService(
      service = new ApiCommandService(
        transactionServices,
        submissionTracker,
        errorCodesVersionSwitcher,
      ),
      ledgerId = configuration.ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationTime),
      generateSubmissionId = SubmissionIdGenerator.Random,
      validator = validator,
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
      Tracking.Key(
        commands.applicationId.unwrap,
        commands.actAs,
      )
    }

    def newTracker(
        configuration: Configuration,
        submissionFlow: SubmissionFlow,
        completionServices: CommandCompletionService,
        metrics: Metrics,
        errorFactories: ErrorFactories,
        offsetValidator: LedgerOffsetValidator,
    )(
        key: Tracking.Key
    )(implicit
        materializer: Materializer,
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Tracker[InternalCommandSubmission]] = {
      // Note: command completions are returned as long as at least one of the original submitters
      // is specified in the command completion request.
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)
      for {
        ledgerEnd <- completionServices.getLedgerEnd(configuration.ledgerId)
      } yield {
        val commandTrackerFlow =
          CommandTrackerFlow[Promise[
            Either[CompletionFailure, CompletionSuccess]
          ], NotUsed, InternalCommandSubmission](
            commandSubmissionFlow = submissionFlow,
            createCommandCompletionSource = offset =>
              offsetValidator
                .validate(offset, "command_tracker_offset")
                .fold(
                  Source.failed,
                  offset =>
                    completionServices
                      .completionStreamSource(
                        CompletionStreamRequest(
                          configuration.ledgerId,
                          ApplicationId(key.applicationId),
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
          errorFactories = errorFactories,
        )
      }
    }
  }
}
