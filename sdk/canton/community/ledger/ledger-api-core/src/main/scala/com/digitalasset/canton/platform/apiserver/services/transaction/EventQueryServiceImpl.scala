// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.transaction

import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.messages.event.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.digitalasset.canton.ledger.api.services.EventQueryService
import com.digitalasset.canton.ledger.api.validation.PartyNameChecker
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexEventQueryService
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.{
  ApiConversions,
  ApiEventQueryService,
  logging,
}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object EventQueryServiceImpl {
  def create(
      ledgerId: LedgerId,
      eventQueryService: IndexEventQueryService,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): ApiEventQueryService with BindableService =
    new ApiEventQueryService(
      new EventQueryServiceImpl(eventQueryService, loggerFactory),
      ledgerId,
      PartyNameChecker.AllowAllParties,
      telemetry,
      loggerFactory,
    )
}

private[apiserver] final class EventQueryServiceImpl private (
    eventQueryService: IndexEventQueryService,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends EventQueryService
    with NamedLogging {

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse] = {

    withEnrichedLoggingContext(
      logging.contractId(request.contractId),
      logging.parties(request.requestingParties),
    ) { implicit loggingContext =>
      logger.info(
        s"Received request for events by contract ID, ${loggingContext.serializeFiltered("contractId", "parties")}."
      )
    }
    logger.trace(s"Events by contract ID request: $request.")

    eventQueryService
      .getEventsByContractId(
        request.contractId,
        request.requestingParties,
      )
      .map(ApiConversions.toV1)
      .andThen(logger.logErrorsOnCall[GetEventsByContractIdResponse])
  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] = {

    withEnrichedLoggingContext(
      logging.contractKey(request.contractKey),
      logging.templateId(request.templateId),
      logging.parties(request.requestingParties),
      logging.keyContinuationToken(request.keyContinuationToken),
    ) { implicit loggingContext =>
      logger.info(s"Received request for events by contract key, ${loggingContext
          .serializeFiltered("contractKey", "templateId", "parties", "eventSequentialId")}.")
    }
    logger.trace(s"Events by contract key request: $request.")

    eventQueryService
      .getEventsByContractKey(
        request.contractKey,
        request.templateId,
        request.requestingParties,
        request.keyContinuationToken,
      )
      .andThen(logger.logErrorsOnCall[GetEventsByContractKeyResponse])
  }

}
