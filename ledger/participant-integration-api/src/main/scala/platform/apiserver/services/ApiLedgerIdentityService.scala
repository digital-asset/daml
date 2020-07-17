// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.{
  LedgerIdentityService => GrpcLedgerIdentityService
}
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ApiException
import io.grpc.{BindableService, ServerServiceDefinition, Status}
import scalaz.syntax.tag._

import scala.concurrent.Future

final class ApiLedgerIdentityService private (getLedgerId: () => Future[LedgerId])(
    implicit logCtx: LoggingContext)
    extends GrpcLedgerIdentityService
    with GrpcApiService {

  @volatile var closed = false

  private val logger = ContextualizedLogger.get(this.getClass)

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
    if (closed)
      Future.failed(
        new ApiException(
          Status.UNAVAILABLE
            .withDescription("Ledger Identity Service closed.")))
    else {
      getLedgerId()
        .map(ledgerId => GetLedgerIdentityResponse(ledgerId.unwrap))(DirectExecutionContext)
        .andThen(logger.logErrorsOnCall[GetLedgerIdentityResponse])(DirectExecutionContext)
    }

  override def close(): Unit = closed = true

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, DirectExecutionContext)
}

object ApiLedgerIdentityService {
  def create(getLedgerId: () => Future[LedgerId])(
      implicit logCtx: LoggingContext): ApiLedgerIdentityService with BindableService = {
    new ApiLedgerIdentityService(getLedgerId)
  }
}
