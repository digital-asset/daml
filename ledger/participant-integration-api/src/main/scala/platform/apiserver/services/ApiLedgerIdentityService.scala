// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.error.{DamlContextualizedErrorLogger, ContextualizedErrorLogger}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.{
  LedgerIdentityService => GrpcLedgerIdentityService
}
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{BindableService, ServerServiceDefinition}
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiLedgerIdentityService private (
    getLedgerId: () => Future[LedgerId]
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends GrpcLedgerIdentityService
    with GrpcApiService {
  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  @volatile var closed = false

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] = {
    logger.info(s"Received request for ledger identity: $request")
    if (closed)
      Future.failed(ErrorFactories.serviceNotRunning(None))
    else
      getLedgerId()
        .map(ledgerId => GetLedgerIdentityResponse(ledgerId.unwrap))
        .andThen(logger.logErrorsOnCall[GetLedgerIdentityResponse])
  }

  override def close(): Unit = closed = true

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiLedgerIdentityService {
  def create(
      getLedgerId: () => Future[LedgerId]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ApiLedgerIdentityService with BindableService = {
    new ApiLedgerIdentityService(getLedgerId)
  }
}
