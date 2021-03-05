// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

object DummyLedgerIdentityService {

  def bind(executionContext: ExecutionContext): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(new DummyLedgerIdentityService, executionContext)

}

final class DummyLedgerIdentityService private extends LedgerIdentityService {
  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] =
    Future.successful(GetLedgerIdentityResponse.of("dummy"))
}
