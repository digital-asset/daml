// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
private[daml] final class LedgerIdentityServiceAuthorization(
    protected val service: LedgerIdentityServiceGrpc.LedgerIdentityService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends LedgerIdentityServiceGrpc.LedgerIdentityService
    with ProxyCloseable
    with GrpcApiService {

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] =
    authorizer.requirePublicClaims(service.getLedgerIdentity)(request)

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
