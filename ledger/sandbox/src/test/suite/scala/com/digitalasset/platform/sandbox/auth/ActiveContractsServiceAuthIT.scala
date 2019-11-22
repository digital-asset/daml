// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.SandboxFixtureWithAuth
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class ActiveContractsServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers
    with Expect {

  private val subscriber = "alice"

  private def getActiveContracts(token: Option[String]): Future[Unit] =
    streamResult[GetActiveContractsResponse](
      observer =>
        stub(ActiveContractsServiceGrpc.stub(channel), token)
          .getActiveContracts(
            new GetActiveContractsRequest(unwrappedLedgerId, txFilterFor(subscriber)),
            observer))

  behavior of "ActiveContractsService with authorization"

  it should "deny unauthorized calls" in {
    expect(getActiveContracts(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(getActiveContracts(Option(rwToken("bob").asHeader(UUID.randomUUID.toString)))).toBeDenied
  }
  it should "deny calls with an invalid signature" in {
    expect(getActiveContracts(Option(rwToken(subscriber).asHeader(UUID.randomUUID.toString)))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getActiveContracts(Option(rwToken(subscriber).asHeader()))).toSucceed
  }
  it should "allow authenticated calls with read-only token" in {
    expect(getActiveContracts(Option(roToken(subscriber).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(getActiveContracts(Option(rwToken(subscriber).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(getActiveContracts(Option(rwToken(subscriber).expiresTomorrow.asHeader()))).toSucceed
  }

}
