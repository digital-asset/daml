// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.SandboxFixtureWithAuth
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class LedgerIdentityServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers
    with Expect {

  private def getLedgerId(token: Option[String]): Future[GetLedgerIdentityResponse] =
    stub(LedgerIdentityServiceGrpc.stub(channel), token)
      .getLedgerIdentity(GetLedgerIdentityRequest())

  behavior of "LedgerIdentityService with authorization"

  it should "deny unauthorized calls" in {
    expect(getLedgerId(None)).toBeDenied
  }
  it should "deny calls with an invalid signature" in {
    expect(getLedgerId(Option(rwToken("alice").asHeader(UUID.randomUUID.toString)))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getLedgerId(Option(rwToken("alice").asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(getLedgerId(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(getLedgerId(Option(rwToken("alice").expiresTomorrow.asHeader()))).toSucceed
  }

}
