// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.event_query_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.services.EventQueryService
import com.digitalasset.canton.ledger.api.validation.EventQueryServiceRequestValidator.KeyTypeValidator
import com.digitalasset.canton.ledger.api.validation.{
  EventQueryServiceRequestValidator,
  PartyNameChecker,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class ApiEventQueryService(
    protected val service: EventQueryService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    keyTypeValidator: KeyTypeValidator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends EventQueryServiceGrpc.EventQueryService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  private val validator = new EventQueryServiceRequestValidator(partyNameChecker, keyTypeValidator)

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {

    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    validator
      .validateEventsByContractId(request)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        service.getEventsByContractId,
      )

  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] = {

    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    for {
      validated <- validator.validateEventsByContractKey(request)
      response <- validated.fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        service.getEventsByContractKey,
      )
    } yield response

  }

  override def bindService(): ServerServiceDefinition =
    EventQueryServiceGrpc.bindService(this, executionContext)

}
