// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
final class LedgerIdentityServiceAuthorization(
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
