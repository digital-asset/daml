// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.admin.command_inspection_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.services.CommandInspectionService
import com.digitalasset.canton.ledger.api.validation.CommandInspectionServiceRequestValidator
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class ApiCommandInspectionService(
    service: CommandInspectionService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends CommandInspectionServiceGrpc.CommandInspectionService
    with StreamingServiceLifecycleManagement
    with NamedLogging {

  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    ErrorLoggingContext(
      logger,
      loggerFactory.properties,
      TraceContext.empty,
    )

  override def getCommandStatus(
      request: GetCommandStatusRequest
  ): Future[GetCommandStatusResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    logger.info(s"Received new command status request $request.")
    CommandInspectionServiceRequestValidator
      .validateCommandStatusRequest(request)(
        ErrorLoggingContext(
          logger,
          loggingContextWithTrace.toPropertiesMap,
          loggingContextWithTrace.traceContext,
        )
      )
      .fold(
        t =>
          Future.failed[GetCommandStatusResponse](
            ValidationLogger.logFailureWithTrace(logger, request, t)
          ),
        _ =>
          service
            .findCommandStatus(request.commandIdPrefix, request.state, request.limit)
            .map(statuses => GetCommandStatusResponse(commandStatus = statuses.map(_.toProto))),
      )
  }

}
