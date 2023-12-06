// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionResponse,
  GetTransactionTreeResponse,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.config
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  SubmitAndWaitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.platform.apiserver.services.ApiCommandServiceV2.TransactionServices
import com.digitalasset.canton.platform.apiserver.services.command.CommandServiceImpl
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.empty.Empty
import io.grpc.Context

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

final class ApiCommandServiceV2(
    transactionServices: TransactionServices,
    commandsValidator: CommandsValidator,
    submissionTracker: SubmissionTracker,
    submit: Traced[SubmitRequest] => Future[Any],
    defaultTrackingTimeout: config.NonNegativeFiniteDuration,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends CommandServiceGrpc.CommandService
    with AutoCloseable
    with NamedLogging {

  private val running = new AtomicBoolean(true)

  private[this] val validator = new SubmitAndWaitRequestValidator(commandsValidator)

  override def close(): Unit = {
    logger.info("Shutting down Command Service.")(TraceContext.empty)
    running.set(false)
    submissionTracker.close()
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    withCommandsLoggingContext(request.getCommands) { (errorLogger, loggingContextWithTrace) =>
      submitAndWaitInternal(request)(errorLogger, loggingContextWithTrace).map { _ =>
        Empty.defaultInstance
      }
    }

  override def submitAndWaitForUpdateId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForUpdateIdResponse] =
    withCommandsLoggingContext(request.getCommands) { (errorLogger, loggingContextWithTrace) =>
      submitAndWaitInternal(request)(errorLogger, loggingContextWithTrace).map { response =>
        SubmitAndWaitForUpdateIdResponse.of(
          updateId = response.completion.updateId,
          completionOffset = offsetFromCheckpoint(response.checkpoint),
        )
      }
    }

  private def offsetFromCheckpoint(checkpoint: Option[Checkpoint]) =
    checkpoint.flatMap(_.offset).flatMap(_.value.absolute).getOrElse("")

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    withCommandsLoggingContext(request.getCommands) { (errorLogger, loggingContextWithTrace) =>
      submitAndWaitInternal(request)(errorLogger, loggingContextWithTrace).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveActAs(request.getCommands)
        val txRequest = GetTransactionByIdRequest(
          updateId = resp.completion.updateId,
          requestingParties = effectiveActAs.toList,
        )
        transactionServices
          .getTransactionById(txRequest)
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
    withCommandsLoggingContext(request.getCommands) { (errorLogger, loggingContextWithTrace) =>
      submitAndWaitInternal(request)(errorLogger, loggingContextWithTrace).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          updateId = resp.completion.updateId,
          requestingParties = effectiveActAs.toList,
        )
        transactionServices
          .getTransactionTreeById(txRequest)
          .map(resp =>
            SubmitAndWaitForTransactionTreeResponse
              .of(resp.transaction, resp.transaction.map(_.offset).getOrElse(""))
          )
      }
    }

  private def submitAndWaitInternal(
      req: SubmitAndWaitRequest
  )(implicit
      errorLogger: ContextualizedErrorLogger,
      loggingContextWithTrace: LoggingContextWithTrace,
  ): Future[CompletionResponse] =
    if (running.get()) {
      // Capture deadline before thread switching in Future for-comprehension
      val deadlineO = Option(Context.current().getDeadline)
      enrichRequestAndSubmit(req) { request =>
        for {
          _ <- Future.unit
          commands = request.commands.getOrElse(
            throw new IllegalArgumentException("Missing commands field in request")
          )
          nonNegativeTimeout <- Future.fromTry(
            CommandServiceImpl.validateRequestTimeout(
              deadlineO,
              commands.commandId,
              commands.submissionId,
              defaultTrackingTimeout,
            )
          )
          result <- submissionTracker.track(
            submissionKey = SubmissionKey(
              commandId = commands.commandId,
              submissionId = commands.submissionId,
              applicationId = commands.applicationId,
              parties = CommandsValidator.effectiveActAs(commands),
            ),
            timeout = nonNegativeTimeout,
            submit = childContext => submit(Traced(SubmitRequest(Some(commands)))(childContext)),
          )(errorLogger, loggingContextWithTrace.traceContext)
        } yield result
      }
    } else
      Future.failed(
        CommonErrors.ServiceNotRunning.Reject("Command Service")(errorLogger).asGrpcError
      )

  private def withCommandsLoggingContext[T](commands: Commands)(
      submitWithContext: (ContextualizedErrorLogger, LoggingContextWithTrace) => Future[T]
  ): Future[T] = {
    val traceContext = getAnnotedCommandTraceContextV2(Some(commands), telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
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
        loggingContext,
      )
    }
  }

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitRequest
  )(
      submit: SubmitAndWaitRequest => Future[T]
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[T] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        ApiConversions.toV1(
          requestWithSubmissionId
        ), // it is enough to validate V1 only, since at submission the SubmitRequest will be validated again (and the domainId as well)
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId),
      )
  }

  private def generateSubmissionIdIfEmpty(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    if (request.commands.exists(_.submissionId.isEmpty)) {
      val commandsWithSubmissionId =
        request.commands.map(_.copy(submissionId = generateSubmissionId.generate()))
      request.copy(commands = commandsWithSubmissionId)
    } else {
      request
    }

  private def contextualizedErrorLogger(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))
}

object ApiCommandServiceV2 {
  final class TransactionServices(
      val getTransactionTreeById: GetTransactionByIdRequest => Future[GetTransactionTreeResponse],
      val getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
  )
}
