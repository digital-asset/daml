// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.LedgerIdentityServiceAuthorization
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class LedgerIdentityServiceImpl private (getResponse: () => Future[String])(implicit
    ec: ExecutionContext
) extends LedgerIdentityService
    with FakeAutoCloseable {

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] = {
    getResponse().map(ledgerId => GetLedgerIdentityResponse(ledgerId))
  }
}

object LedgerIdentityServiceImpl {

  def createWithRef(getResponse: () => Future[String], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, LedgerIdentityServiceImpl) = {
    val impl = new LedgerIdentityServiceImpl(
      getResponse
    )
    val authImpl = new LedgerIdentityServiceAuthorization(impl, authorizer)
    (LedgerIdentityServiceGrpc.bindService(authImpl, ec), impl): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )
  }
}
