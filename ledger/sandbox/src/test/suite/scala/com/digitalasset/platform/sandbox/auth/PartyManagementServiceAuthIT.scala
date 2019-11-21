// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.SandboxFixtureWithAuth
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class PartyManagementServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers
    with Expect {

  private def listKnownParties(token: Option[String]): Future[ListKnownPartiesResponse] =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .listKnownParties(ListKnownPartiesRequest())

  behavior of "PartyManagementService#ListKnownParties with authorization"

  it should "deny unauthorized calls" in {
    expect(listKnownParties(None)).toBeDenied
  }
  it should "deny authenticated calls for a user with public claims" in {
    expect(listKnownParties(Option(rwToken("alice").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls for an admin" in {
    expect(listKnownParties(Option(adminToken.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for a user with public claims" in {
    expect(listKnownParties(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "deny calls with non-expired tokens for a user with public claims" in {
    expect(listKnownParties(Option(rwToken("alice").expiresTomorrow.asHeader()))).toBeDenied
  }
  it should "deny calls with expired tokens for an admin" in {
    expect(listKnownParties(Option(adminToken.expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for an admin" in {
    expect(listKnownParties(Option(adminToken.expiresTomorrow.asHeader()))).toSucceed
  }

  private def allocateParty(token: Option[String]): Future[AllocatePartyResponse] =
    stub(PartyManagementServiceGrpc.stub(channel), token).allocateParty(AllocatePartyRequest())

  behavior of "PartyManagementService#AllocateParty with authorization"

  it should "deny unauthorized calls" in {
    expect(allocateParty(None)).toBeDenied
  }
  it should "deny authenticated calls for a user with public claims" in {
    expect(allocateParty(Option(rwToken("alice").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls for an admin" in {
    expect(allocateParty(Option(adminToken.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for a user with public claims" in {
    expect(allocateParty(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "deny calls with non-expired tokens for a user with public claims" in {
    expect(allocateParty(Option(rwToken("alice").expiresTomorrow.asHeader()))).toBeDenied
  }
  it should "deny calls with expired tokens for an admin" in {
    expect(allocateParty(Option(adminToken.expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for an admin" in {
    expect(allocateParty(Option(adminToken.expiresTomorrow.asHeader()))).toSucceed
  }

}
