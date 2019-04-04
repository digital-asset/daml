// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class LedgerIdentityServiceImpl(ledgerId: String) extends LedgerIdentityService {

  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
    Future.successful(GetLedgerIdentityResponse(ledgerId))

}

object LedgerIdentityServiceImpl {
  def createWithRef(ledgerId: String)(
      implicit ec: ExecutionContext): (ServerServiceDefinition, LedgerIdentityServiceImpl) = {
    val serviceImpl = new LedgerIdentityServiceImpl(ledgerId)
    (LedgerIdentityServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
