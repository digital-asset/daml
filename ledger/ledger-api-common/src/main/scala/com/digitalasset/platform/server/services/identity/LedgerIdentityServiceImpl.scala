// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.identity

import com.daml.ledger.participant.state.index.v1.IdentityService
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
import io.grpc.{BindableService, ServerServiceDefinition, Status}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

abstract class LedgerIdentityServiceImpl private (identityService: IdentityService)
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
      identityService.getLedgerId().map(GetLedgerIdentityResponse(_))(DirectExecutionContext)

  override def close(): Unit = closed = true

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, DirectExecutionContext)
}

object LedgerIdentityServiceImpl {
  def apply(identityService: IdentityService)
    : LedgerIdentityService with BindableService with LedgerIdentityServiceLogging =
    new LedgerIdentityServiceImpl(identityService) with LedgerIdentityServiceLogging
}
