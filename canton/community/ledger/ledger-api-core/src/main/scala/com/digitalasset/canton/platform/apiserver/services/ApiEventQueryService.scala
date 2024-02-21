// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.api.v2.event_query_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.{
  EventQueryServiceRequestValidator,
  PartyNameChecker,
}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexEventQueryService
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}

final class ApiEventQueryService(
    eventQueryService: IndexEventQueryService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends EventQueryServiceGrpc.EventQueryService
    with GrpcApiService
    with NamedLogging {

  private val validator = new EventQueryServiceRequestValidator(PartyNameChecker.AllowAllParties)

  override def getEventsByContractId(
      req: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)
    validator
      .validateEventsByContractId(req)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          withEnrichedLoggingContext(
            logging.contractId(request.contractId),
            logging.parties(request.requestingParties),
          ) { implicit loggingContext =>
            logger.info("Received request for events by contract ID")
          }
          logger.trace(s"Events by contract ID request: $request")
          eventQueryService
            .getEventsByContractId(
              request.contractId,
              request.requestingParties,
            )
            .andThen(logger.logErrorsOnCall[GetEventsByContractIdResponse])
        },
      )
  }

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    EventQueryServiceGrpc.bindService(this, executionContext)
}
