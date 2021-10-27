// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.error.definitions.RejectionGenerators
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.{
  CommandSubmissionService => ApiCommandSubmissionService
}
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest => ApiSubmitRequest,
}
import com.daml.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import com.daml.telemetry.{DefaultTelemetry, SpanAttribute, TelemetryContext}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandSubmissionService(
    override protected val service: CommandSubmissionService with AutoCloseable,
    ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration],
    metrics: Metrics,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(
      logger,
      loggingContext,
      None,
    )
  private val validator = new SubmitRequestValidator(
    new CommandsValidator(ledgerId, errorCodesVersionSwitcher),
    FieldValidations(ErrorFactories(errorCodesVersionSwitcher)),
  )

  override def submit(request: ApiSubmitRequest): Future[Empty] = {
    implicit val telemetryContext: TelemetryContext =
      DefaultTelemetry.contextFromGrpcThreadLocalContext()
    request.commands.foreach { commands =>
      telemetryContext.setAttribute(SpanAttribute.ApplicationId, commands.applicationId)
      telemetryContext.setAttribute(SpanAttribute.CommandId, commands.commandId)
      telemetryContext.setAttribute(SpanAttribute.Submitter, commands.party)
      telemetryContext.setAttribute(SpanAttribute.WorkflowId, commands.workflowId)
    }
    Timed.timedAndTrackedFuture(
      metrics.daml.commands.submissions,
      metrics.daml.commands.submissionsRunning,
      Timed
        .value(
          metrics.daml.commands.validation,
          validator.validate(
            request,
            currentLedgerTime(),
            currentUtcTime(),
            maxDeduplicationTime(),
          ),
        )
        .fold(
          t =>
            Future.failed {
              ValidationLogger.logFailure(request, t)
              RejectionGenerators.validationFailure(t)
            },
          service.submit(_).map(_ => Empty.defaultInstance),
        ),
    )
  }

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)
}
