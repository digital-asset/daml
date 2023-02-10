// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.event.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.ledger.participant.state.index.v2.IndexEventQueryService
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.logging
import com.daml.platform.server.api.services.domain.EventQueryService
import com.daml.platform.server.api.services.grpc.GrpcEventQueryService
import com.daml.tracing.Telemetry
import io.grpc._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] object ApiEventQueryService {
  def create(
      ledgerId: LedgerId,
      eventQueryService: IndexEventQueryService,
      telemetry: Telemetry,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): GrpcEventQueryService with BindableService =
    new GrpcEventQueryService(
      new ApiEventQueryService(eventQueryService),
      ledgerId,
      PartyNameChecker.AllowAllParties,
      telemetry,
    )
}

private[apiserver] final class ApiEventQueryService private (
    eventQueryService: IndexEventQueryService
)(implicit executionContext: ExecutionContext)
    extends EventQueryService {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractIdResponse] = {

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
  }

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractKeyResponse] = {

    withEnrichedLoggingContext(
      logging.contractKey(request.contractKey),
      logging.templateId(request.templateId),
      logging.parties(request.requestingParties),
      logging.eventSequentialId(request.endExclusiveSeqId),
    ) { implicit loggingContext =>
      logger.info("Received request for events by contract key")
    }
    logger.trace(s"Events by contract key request: $request")

    eventQueryService
      .getEventsByContractKey(
        request.contractKey,
        request.templateId,
        request.requestingParties,
        request.endExclusiveSeqId,
      )
      .andThen(logger.logErrorsOnCall[GetEventsByContractKeyResponse])
  }

}
