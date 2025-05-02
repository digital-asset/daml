// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService as CommandServiceGrpc
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.reassignment_commands.ReassignmentCommands
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TransactionFormat,
  WildcardFilter,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandService
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  SubmitAndWaitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{ProxyCloseable, SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandService(
    protected val service: CommandService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: Duration,
    generateSubmissionId: SubmissionIdGenerator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandServiceGrpc
    with GrpcApiService
    with ProxyCloseable
    with NamedLogging {

  private[this] val validator = new SubmitAndWaitRequestValidator(commandsValidator)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] =
    enrichRequestAndSubmit(request = request)(service.submitAndWait)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    enrichRequestAndSubmit(request = request)(service.submitAndWaitForTransaction)

  override def submitAndWaitForReassignment(
      request: SubmitAndWaitForReassignmentRequest
  ): Future[SubmitAndWaitForReassignmentResponse] =
    enrichRequestAndSubmit(request = request)(service.submitAndWaitForReassignment)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    enrichRequestAndSubmit(request = request)(
      service.submitAndWaitForTransactionTree
    )

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitRequest
  )(submit: SubmitAndWaitRequest => LoggingContextWithTrace => Future[T]): Future[T] = {
    val traceContext = getAnnotatedCommandTraceContext(request.commands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionId =
      request.update(_.optionalCommands.modify(generateSubmissionIdIfEmpty))
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration,
      )(errorLoggingContext(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId)(loggingContext),
      )
  }

  private def enrichRequestAndSubmit(
      request: SubmitAndWaitForTransactionRequest
  )(
      submit: SubmitAndWaitForTransactionRequest => LoggingContextWithTrace => Future[
        SubmitAndWaitForTransactionResponse
      ]
  ): Future[SubmitAndWaitForTransactionResponse] = {
    val traceContext = getAnnotatedCommandTraceContext(request.commands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionIdAndFormat =
      request
        .update(_.optionalCommands.modify(generateSubmissionIdIfEmpty))
        .update(
          _.optionalTransactionFormat
            .modify(
              generateTransactionFormatIfEmpty(
                request.commands.toList.flatMap(cmds => cmds.actAs ++ cmds.readAs)
              )
            )
        )
    validator
      .validate(
        requestWithSubmissionIdAndFormat,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration,
      )(errorLoggingContext(requestWithSubmissionIdAndFormat))
      .fold(
        t =>
          Future.failed(
            ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionIdAndFormat, t)
          ),
        _ => submit(requestWithSubmissionIdAndFormat)(loggingContext),
      )
  }

  private def enrichRequestAndSubmit(
      request: SubmitAndWaitForReassignmentRequest
  )(
      submit: SubmitAndWaitForReassignmentRequest => LoggingContextWithTrace => Future[
        SubmitAndWaitForReassignmentResponse
      ]
  ): Future[SubmitAndWaitForReassignmentResponse] = {
    val traceContext =
      getAnnotatedReassignmentCommandTraceContext(request.reassignmentCommands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionId =
      request.update(_.optionalReassignmentCommands.modify(generateSubmissionIdIfEmptyReassignment))
    validator
      .validate(
        requestWithSubmissionId
      )(errorLoggingContext(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ =>
          requestWithSubmissionId.eventFormat match {
            case Some(_) =>
              submit(requestWithSubmissionId)(loggingContext)
            case None =>
              // request reassignment for all parties and remove the events
              submit(
                requestWithSubmissionId.copy(eventFormat =
                  Some(
                    EventFormat(
                      filtersByParty = Map.empty,
                      filtersForAnyParty = Some(Filters(Nil)),
                      verbose = false,
                    )
                  )
                )
              )(
                loggingContext
              ).map(_.update(_.reassignment.modify(_.clearEvents)))
          },
      )
  }

  private def generateSubmissionIdIfEmpty(commands: Option[Commands]): Option[Commands] =
    if (commands.exists(_.submissionId.isEmpty)) {
      commands.map(_.copy(submissionId = generateSubmissionId.generate()))
    } else {
      commands
    }

  private def generateTransactionFormatIfEmpty(
      actAs: Seq[String]
  )(transactionFormat: Option[TransactionFormat]): Option[TransactionFormat] = {
    val wildcard = Filters(
      cumulative = Seq(
        CumulativeFilter(
          IdentifierFilter.WildcardFilter(
            WildcardFilter(false)
          )
        )
      )
    )
    transactionFormat.orElse(
      Some(
        TransactionFormat(
          eventFormat =
            Some(EventFormat(actAs.map(party => party -> wildcard).toMap, None, verbose = true)),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      )
    )
  }

  private def generateSubmissionIdIfEmptyReassignment(
      commands: Option[ReassignmentCommands]
  ): Option[ReassignmentCommands] =
    if (commands.exists(_.submissionId.isEmpty)) {
      commands.map(_.copy(submissionId = generateSubmissionId.generate()))
    } else {
      commands
    }

  private def errorLoggingContext(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))

  private def errorLoggingContext(request: SubmitAndWaitForTransactionRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))

  private def errorLoggingContext(request: SubmitAndWaitForReassignmentRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(
      logger,
      loggingContext,
      request.reassignmentCommands.map(_.submissionId),
    )
}
