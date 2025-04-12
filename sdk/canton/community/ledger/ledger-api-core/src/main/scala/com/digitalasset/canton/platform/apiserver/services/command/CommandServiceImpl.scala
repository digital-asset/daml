// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_submission_service.{
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.reassignment_commands.ReassignmentCommands
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{Filters, UpdateFormat}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionTreeResponse,
  GetUpdateByIdRequest,
  GetUpdateResponse,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.config
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandService
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.services.command.CommandServiceImpl.*
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.digitalasset.canton.platform.apiserver.services.{ApiCommandService, logging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.grpc.{Context, Deadline, Status}

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] final class CommandServiceImpl private[services] (
    updateServices: UpdateServices,
    transactionSubmissionTracker: SubmissionTracker,
    reassignmentSubmissionTracker: SubmissionTracker,
    submit: Traced[SubmitRequest] => FutureUnlessShutdown[SubmitResponse],
    submitReassignment: Traced[SubmitReassignmentRequest] => FutureUnlessShutdown[
      SubmitReassignmentResponse
    ],
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
    transactionSubmissionTracker.close()
    reassignmentSubmissionTracker.close()
  }

  def submitAndWait(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request.commands)(errorLogger, traceContext).map { response =>
        SubmitAndWaitResponse.of(
          updateId = response.completion.updateId,
          completionOffset = response.completion.offset,
        )
      }
    }

  def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request.commands)(errorLogger, traceContext).flatMap { resp =>
        val updateId = resp.completion.updateId
        val txRequest = GetUpdateByIdRequest(
          updateId = updateId,
          updateFormat = Some(
            UpdateFormat(
              includeTransactions = request.transactionFormat,
              includeReassignments = None,
              includeTopologyEvents = None,
            )
          ),
        )
        updateServices
          .getUpdateById(txRequest)
          .recoverWith {
            case e: io.grpc.StatusRuntimeException
                if e.getStatus.getCode == Status.Code.NOT_FOUND
                  && e.getStatus.getDescription.contains(
                    RequestValidationErrors.NotFound.Update.id
                  ) =>
              logger.debug(
                s"Transaction not found in update lookup for updateId $updateId, falling back to LedgerEffects lookup without events."
              )(traceContext)
              // When a command submission completes successfully,
              // the submitters can end up getting an UPDATE_NOT_FOUND when querying its corresponding AcsDelta
              // transaction that either:
              // * has only non-consuming events
              // * has only events of contracts which have stakeholders that are not amongst the requesting parties
              // or in general when filters defined in the transactionFormat exclude all the events from the
              // transaction.
              // In these situations, we fallback to a LedgerEffects transaction lookup with a wildcard filter and
              // populate the transaction response  with its details but no events.
              updateServices
                .getUpdateById(
                  txRequest
                    .update(
                      _.updateFormat.includeTransactions.transactionShape := TRANSACTION_SHAPE_LEDGER_EFFECTS,
                      _.updateFormat.includeTransactions.eventFormat.filtersForAnyParty := Filters(
                        Nil
                      ),
                    )
                )
                .map(_.update(_.transaction.modify(_.clearEvents)))
          }
          .map(updateResponse =>
            SubmitAndWaitForTransactionResponse
              .of(
                updateResponse.update.transaction
              )
          )
      }
    }

  def submitAndWaitForReassignment(
      request: SubmitAndWaitForReassignmentRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForReassignmentResponse] =
    withReassignmentCommandsLoggingContext(request.getReassignmentCommands, loggingContext) {
      (errorLogger, traceContext) =>
        submitAndWaitForReassignmentInternal(request.reassignmentCommands)(
          errorLogger,
          traceContext,
        )
          .flatMap { resp =>
            val updateId = resp.completion.updateId
            val txRequest = GetUpdateByIdRequest(
              updateId = updateId,
              updateFormat = Some(
                UpdateFormat(
                  includeTransactions = None,
                  includeReassignments = request.eventFormat,
                  includeTopologyEvents = None,
                )
              ),
            )
            updateServices
              .getUpdateById(txRequest)
              .map(updateResponse =>
                SubmitAndWaitForReassignmentResponse
                  .of(
                    updateResponse.update.reassignment
                  )
              )
          }
    }

  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionTreeResponse] =
    withCommandsLoggingContext(request.getCommands, loggingContext) { (errorLogger, traceContext) =>
      submitAndWaitInternal(request.commands)(errorLogger, traceContext).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          updateId = resp.completion.updateId,
          requestingParties = effectiveActAs.toList,
          transactionFormat = None,
        )
        updateServices
          .getTransactionTreeById(txRequest)
          .map(resp => SubmitAndWaitForTransactionTreeResponse.of(resp.transaction))
      }
    }

  private def submitAndWaitInternal(
      commands: Option[Commands]
  )(implicit
      errorLogger: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[CompletionResponse] = {
    def ifServiceRunning: Future[Unit] =
      if (!running.get())
        Future.failed(
          CommonErrors.ServiceNotRunning.Reject("Command Service")(errorLogger).asGrpcError
        )
      else Future.unit

    def ensureCommandsPopulated: Commands =
      commands.getOrElse(
        throw new IllegalArgumentException("Missing commands field in request")
      )

    def submitAndTrack(
        commands: Commands,
        nonNegativeTimeout: config.NonNegativeFiniteDuration,
    ): Future[CompletionResponse] =
      transactionSubmissionTracker.track(
        submissionKey = SubmissionKey(
          commandId = commands.commandId,
          submissionId = commands.submissionId,
          userId = commands.userId,
          parties = commands.actAs.toSet,
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

  private def submitAndWaitForReassignmentInternal(
      commands: Option[ReassignmentCommands]
  )(implicit
      errorLogger: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[CompletionResponse] = {
    def ifServiceRunning: Future[Unit] =
      if (!running.get())
        Future.failed(
          CommonErrors.ServiceNotRunning.Reject("Command Service")(errorLogger).asGrpcError
        )
      else Future.unit

    def ensureCommandsPopulated: ReassignmentCommands =
      commands.getOrElse(
        throw new IllegalArgumentException("Missing commands field in request")
      )

    def submitAndTrack(
        commands: ReassignmentCommands,
        nonNegativeTimeout: config.NonNegativeFiniteDuration,
    ): Future[CompletionResponse] =
      reassignmentSubmissionTracker.track(
        submissionKey = SubmissionKey.fromReassignmentCommands(commands),
        timeout = nonNegativeTimeout,
        submit = childContext =>
          submitReassignment(Traced(SubmitReassignmentRequest(Some(commands)))(childContext)),
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
      submitWithContext: (ErrorLoggingContext, TraceContext) => Future[T]
  ): Future[T] =
    LoggingContextWithTrace.withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { loggingContext =>
      submitWithContext(
        ErrorLoggingContext.withExplicitCorrelationId(
          logger,
          loggingContext.toPropertiesMap,
          loggingContext.traceContext,
          commands.submissionId,
        ),
        loggingContext.traceContext,
      )
    }(loggingContextWithTrace)

  private def withReassignmentCommandsLoggingContext[T](
      commands: ReassignmentCommands,
      loggingContextWithTrace: LoggingContextWithTrace,
  )(
      submitWithContext: (ErrorLoggingContext, TraceContext) => Future[T]
  ): Future[T] =
    LoggingContextWithTrace.withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.submitter(commands.submitter),
    ) { loggingContext =>
      submitWithContext(
        ErrorLoggingContext.withExplicitCorrelationId(
          logger,
          loggingContext.toPropertiesMap,
          loggingContext.traceContext,
          commands.submissionId,
        ),
        loggingContext.traceContext,
      )
    }(loggingContextWithTrace)
}

private[apiserver] object CommandServiceImpl {

  def createApiService(
      transactionSubmissionTracker: SubmissionTracker,
      reassignmentSubmissionTracker: SubmissionTracker,
      commandsValidator: CommandsValidator,
      submit: Traced[SubmitRequest] => FutureUnlessShutdown[SubmitResponse],
      submitReassignment: Traced[SubmitReassignmentRequest] => FutureUnlessShutdown[
        SubmitReassignmentResponse
      ],
      defaultTrackingTimeout: config.NonNegativeFiniteDuration,
      updateServices: UpdateServices,
      timeProvider: TimeProvider,
      maxDeduplicationDuration: config.NonNegativeFiniteDuration,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): CommandServiceGrpc.CommandService & GrpcApiService =
    new ApiCommandService(
      service = new CommandServiceImpl(
        updateServices,
        transactionSubmissionTracker,
        reassignmentSubmissionTracker,
        submit,
        submitReassignment,
        defaultTrackingTimeout,
        loggerFactory,
      ),
      commandsValidator = commandsValidator,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationDuration = maxDeduplicationDuration.asJava,
      generateSubmissionId = SubmissionIdGenerator.Random,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )

  final class UpdateServices(
      val getTransactionTreeById: GetTransactionByIdRequest => Future[GetTransactionTreeResponse],
      val getUpdateById: GetUpdateByIdRequest => Future[GetUpdateResponse],
  )

  private[apiserver] def validateRequestTimeout(
      grpcRequestDeadline: Option[Deadline],
      commandId: String,
      submissionId: String,
      defaultTrackingTimeout: config.NonNegativeFiniteDuration,
  )(implicit errorLogger: ErrorLoggingContext): Try[config.NonNegativeFiniteDuration] =
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
