// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.event_query_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.services.EventQueryService
import com.digitalasset.canton.ledger.api.validation.TransactionServiceRequestValidator.Result
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
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends EventQueryServiceGrpc.EventQueryService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  private val validator = new EventQueryServiceRequestValidator(partyNameChecker)

  private def getSingleResponse[Request, DomainRequest, Response](
      request: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Response] =
    validate(request).fold(
      t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
      fetch(_),
    )

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLogger = ErrorLoggingContext(logger, loggingContextWithTrace)

    getSingleResponse(
      request,
      validator.validateEventsByContractId,
      service.getEventsByContractId,
    )
  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLogger = ErrorLoggingContext(logger, loggingContextWithTrace)

    getSingleResponse(
      request,
      validator.validateEventsByContractKey,
      service.getEventsByContractKey,
    )
  }

  override def bindService(): ServerServiceDefinition =
    EventQueryServiceGrpc.bindService(this, executionContext)

}
