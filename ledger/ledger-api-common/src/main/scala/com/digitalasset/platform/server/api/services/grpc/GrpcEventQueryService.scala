// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.event_query_service._
import com.daml.ledger.api.validation.TransactionServiceRequestValidator.Result
import com.daml.ledger.api.validation.{EventQueryServiceRequestValidator, PartyNameChecker}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.EventQueryService
import com.daml.platform.server.api.services.grpc.Logging.traceId
import com.daml.tracing.Telemetry
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class GrpcEventQueryService(
    protected val service: EventQueryService,
    val ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    telemetry: Telemetry,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends EventQueryServiceGrpc.EventQueryService
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val validator = new EventQueryServiceRequestValidator(partyNameChecker)

  private def getSingleResponse[Request, DomainRequest, Response](
      request: Request,
      validate: Request => Result[DomainRequest],
      fetch: DomainRequest => Future[Response],
  ): Future[Response] =
    validate(request).fold(t => Future.failed(ValidationLogger.logFailure(request, t)), fetch(_))

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleResponse(
          request,
          validator.validateEventsByContractId,
          service.getEventsByContractId,
        )
    }
  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] = {
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        getSingleResponse(
          request,
          validator.validateEventsByContractKey,
          service.getEventsByContractKey,
        )
    }
  }

  override def bindService(): ServerServiceDefinition =
    EventQueryServiceGrpc.bindService(this, executionContext)

}
