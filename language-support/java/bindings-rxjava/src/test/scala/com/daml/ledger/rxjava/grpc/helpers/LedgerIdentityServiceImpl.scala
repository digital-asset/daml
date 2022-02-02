// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services.LedgerIdentityServiceAuthorization
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class LedgerIdentityServiceImpl(ledgerId: String)
    extends LedgerIdentityService
    with FakeAutoCloseable {

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] = {
    Future.successful(GetLedgerIdentityResponse(ledgerId))
  }
}

object LedgerIdentityServiceImpl {

  def createWithRef(ledgerId: String, authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, LedgerIdentityServiceImpl) = {
    val impl = new LedgerIdentityServiceImpl(ledgerId)
    val authImpl = new LedgerIdentityServiceAuthorization(impl, authorizer)
    (LedgerIdentityServiceGrpc.bindService(authImpl, ec), impl): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )
  }
}
