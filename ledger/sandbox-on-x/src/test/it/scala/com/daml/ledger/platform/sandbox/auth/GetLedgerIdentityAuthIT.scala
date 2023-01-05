// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}

import scala.annotation.nowarn
import scala.concurrent.Future

final class GetLedgerIdentityAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "LedgerIdentityService#GetLedgerIdentity"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(LedgerIdentityServiceGrpc.stub(channel), token)
      .getLedgerIdentity(GetLedgerIdentityRequest()): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

}
