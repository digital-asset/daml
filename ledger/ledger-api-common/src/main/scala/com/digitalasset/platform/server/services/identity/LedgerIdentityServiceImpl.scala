// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.identity

import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
  LedgerIdentityServiceLogging
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ApiException
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.{ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

abstract class LedgerIdentityServiceImpl private (val ledgerId: String)
    extends LedgerIdentityService
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(LedgerIdentityService.getClass)

  @volatile var closed = false

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
    if (closed)
      Future.failed(
        new ApiException(
          Status.UNAVAILABLE
            .withDescription("Ledger Identity Service closed.")))
    else
      Future.successful(GetLedgerIdentityResponse(ledgerId))

  override def close(): Unit = closed = true
}

object LedgerIdentityServiceImpl {
  def apply(ledgerId: String): LedgerIdentityServiceImpl with LedgerIdentityServiceLogging =
    new LedgerIdentityServiceImpl(ledgerId) with LedgerIdentityServiceLogging {
      override def bindService(): ServerServiceDefinition =
        LedgerIdentityServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
