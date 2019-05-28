// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.identity

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
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.{BindableService, ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.concurrent.Future

abstract class LedgerIdentityService private (getLedgerId: () => Future[LedgerId])
    extends GrpcLedgerIdentityService
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

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

object LedgerIdentityService {
  def createApiService(getLedgerId: () => Future[LedgerId])
    : LedgerIdentityService with BindableService with LedgerIdentityServiceLogging =
    new LedgerIdentityService(getLedgerId) with LedgerIdentityServiceLogging
}
