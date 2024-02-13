// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_service.*
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse,
}
import com.daml.ledger.api.v2.checkpoint.Checkpoint
import com.daml.tracing.Telemetry
import com.digitalasset.canton.config
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandService
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.{
  LedgerErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.digitalasset.canton.platform.apiserver.services.command.CommandServiceImpl.*
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.digitalasset.canton.platform.apiserver.services.{ApiCommandService, logging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.empty.Empty
import io.grpc.{Context, Deadline}

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] final class CommandServiceImpl private[services] (
    transactionServices: TransactionServices,
    submissionTracker: SubmissionTracker,
    submit: Traced[SubmitRequest] => Future[Empty],
    defaultTrackingTimeout: config.NonNegativeFiniteDuration,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends CommandService
    with AutoCloseable
    with NamedLogging {

  private val running = new AtomicBoolean(true)

  override def close(): Unit = {
    logger.info("Shutting down Command Service.")(TraceContext.empty)
    running.set(false)
    submissionTracker.close()
  }

  def submitAndWait(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[Empty] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request)(errorLogger, traceContext).map { _ =>
        Empty.defaultInstance
      }
    }

  def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionIdResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request)(errorLogger, traceContext).map { response =>
        SubmitAndWaitForTransactionIdResponse.of(
          transactionId = response.completion.updateId,
          completionOffset = offsetFromCheckpoint(response.checkpoint),
        )
      }
    }

  private def offsetFromCheckpoint(checkpoint: Option[Checkpoint]) =
    checkpoint.flatMap(_.offset).flatMap(_.value.absolute).getOrElse("")

  def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request)(errorLogger, traceContext).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          ledgerId = request.getCommands.ledgerId,
          transactionId = resp.completion.updateId,
          requestingParties = effectiveActAs.toList,
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

  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionTreeResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request)(errorLogger, traceContext).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          ledgerId = request.getCommands.ledgerId,
          transactionId = resp.completion.updateId,
          requestingParties = effectiveActAs.toList,
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
      errorLogger: ContextualizedErrorLogger,
      traceContext: TraceContext,
  ): Future[CompletionResponse] = {
    def ifServiceRunning: Future[Unit] =
      if (!running.get())
        Future.failed(
          CommonErrors.ServiceNotRunning.Reject("Command Service")(errorLogger).asGrpcError
        )
      else Future.unit

    def ensureCommandsPopulated: Commands =
      request.commands.getOrElse(
        throw new IllegalArgumentException("Missing commands field in request")
      )

    def submitAndTrack(
        commands: Commands,
        nonNegativeTimeout: config.NonNegativeFiniteDuration,
    ): Future[CompletionResponse] =
      submissionTracker.track(
        submissionKey = SubmissionKey(
          commandId = commands.commandId,
          submissionId = commands.submissionId,
          applicationId = commands.applicationId,
          parties = CommandsValidator.effectiveActAs(commands),
        ),
        timeout = nonNegativeTimeout,
        submit = childContext => submit(Traced(SubmitRequest(Some(commands)))(childContext)),
      )(errorLogger, traceContext)

    // Capture deadline before thread switching in Future for-comprehension
    val deadlineO = Option(Context.current().getDeadline)
    for {
      _ <- ifServiceRunning
      commands = ensureCommandsPopulated
      nonNegativeTimeout <- Future.fromTry(
        validateRequestTimeout(
          deadlineO,
          commands.commandId,
          commands.submissionId,
          defaultTrackingTimeout,
        )(
          errorLogger
        )
      )
      result <- submitAndTrack(commands, nonNegativeTimeout)
    } yield result
  }

  private def withCommandsLoggingContext[T](
      commands: Commands,
      loggingContextWithTrace: LoggingContextWithTrace,
  )(
      submitWithContext: (ContextualizedErrorLogger, TraceContext) => Future[T]
  ): Future[T] = {
    LoggingContextWithTrace.withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.partyString(commands.party),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { loggingContext =>
      submitWithContext(
        LedgerErrorLoggingContext(
          logger,
          loggingContext.toPropertiesMap,
          loggingContext.traceContext,
          commands.submissionId,
        ),
        loggingContext.traceContext,
      )
    }(loggingContextWithTrace)
  }
}

private[apiserver] object CommandServiceImpl {

  def createApiService(
      submissionTracker: SubmissionTracker,
      commandsValidator: CommandsValidator,
      submit: Traced[SubmitRequest] => Future[Empty],
      defaultTrackingTimeout: config.NonNegativeFiniteDuration,
      transactionServices: TransactionServices,
      timeProvider: TimeProvider,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): CommandServiceGrpc.CommandService & GrpcApiService =
    new ApiCommandService(
      service = new CommandServiceImpl(
        transactionServices,
        submissionTracker,
        submit,
        defaultTrackingTimeout,
        loggerFactory,
      ),
      commandsValidator = commandsValidator,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationDuration = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
      generateSubmissionId = SubmissionIdGenerator.Random,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )

  final class TransactionServices(
      val getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
      val getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse],
  )

  private[apiserver] def validateRequestTimeout(
      grpcRequestDeadline: Option[Deadline],
      commandId: String,
      submissionId: String,
      defaultTrackingTimeout: config.NonNegativeFiniteDuration,
  )(implicit errorLogger: ContextualizedErrorLogger): Try[config.NonNegativeFiniteDuration] =
    grpcRequestDeadline.map(_.timeRemaining(TimeUnit.NANOSECONDS)) match {
      case None => Success(defaultTrackingTimeout)
      case Some(remainingDeadlineNanos) if remainingDeadlineNanos >= 0 =>
        Success(
          config.NonNegativeFiniteDuration(Duration(remainingDeadlineNanos, TimeUnit.NANOSECONDS))
        )
      case Some(remainingDeadlineNanos) =>
        Failure(
          CommonErrors.RequestDeadlineExceeded
            .Reject(
              Duration.fromNanos(Math.abs(remainingDeadlineNanos)),
              commandId = commandId,
              submissionId = submissionId,
            )(errorLogger)
            .asGrpcError
        )
    }
}
