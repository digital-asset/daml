// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  TimeServiceGrpc
}
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.SandboxFixtureWithAuth
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class TimeServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers
    with Expect {

  private def getTime(token: Option[String]): Future[Unit] =
    streamResult[GetTimeResponse](
      observer =>
        stub(TimeServiceGrpc.stub(channel), token)
          .getTime(new GetTimeRequest(unwrappedLedgerId), observer))

  behavior of "TimeService with authorization"

  it should "deny unauthorized calls" in {
    expect(getTime(None)).toBeDenied
  }
  it should "deny calls with an invalid signature" in {
    expect(getTime(Option(adminToken.asHeader(UUID.randomUUID.toString)))).toBeDenied
  }
  it should "allow authenticated calls for a user with public claims" in {
    expect(getTime(Option(rwToken("alice").asHeader()))).toSucceed
  }
  it should "allow authenticated calls for an admin" in {
    expect(getTime(Option(adminToken.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for a user with public claims" in {
    expect(getTime(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for a user with public claims" in {
    expect(getTime(Option(rwToken("alice").expiresTomorrow.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for an admin" in {
    expect(getTime(Option(adminToken.expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for an admin" in {
    expect(getTime(Option(adminToken.expiresTomorrow.asHeader()))).toSucceed
  }

}
