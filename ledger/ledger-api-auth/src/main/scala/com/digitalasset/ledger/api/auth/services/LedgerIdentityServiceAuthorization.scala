// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class LedgerIdentityServiceAuthorization(
    protected val service: LedgerIdentityServiceGrpc.LedgerIdentityService with AutoCloseable,
    protected val authService: AuthService)
    extends LedgerIdentityServiceGrpc.LedgerIdentityService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(LedgerIdentityService.getClass)

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.getLedgerIdentity(request))

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
