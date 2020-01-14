// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.{
  LedgerIdentityService => GrpcLedgerIdentityService
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
  LedgerIdentityServiceLogging
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ApiException
import io.grpc.{BindableService, ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.concurrent.Future

abstract class ApiLedgerIdentityService private (
    getLedgerId: () => Future[LedgerId],
    protected val logger: Logger)
    extends GrpcLedgerIdentityService
    with GrpcApiService {

  @volatile var closed = false

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
    if (closed)
      Future.failed(
        new ApiException(
          Status.UNAVAILABLE
            .withDescription("Ledger Identity Service closed.")))
    else
      getLedgerId()
        .map(ledgerId => GetLedgerIdentityResponse(ledgerId.unwrap))(DirectExecutionContext)

  override def close(): Unit = closed = true

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, DirectExecutionContext)
}

object ApiLedgerIdentityService {
  def create(getLedgerId: () => Future[LedgerId])
    : ApiLedgerIdentityService with BindableService with LedgerIdentityServiceLogging = {
    val loggerOverride = LoggerFactory.getLogger(GrpcLedgerIdentityService.getClass)
    new ApiLedgerIdentityService(getLedgerId, loggerOverride) with LedgerIdentityServiceLogging {
      override protected val logger: Logger = loggerOverride
    }
  }
}
