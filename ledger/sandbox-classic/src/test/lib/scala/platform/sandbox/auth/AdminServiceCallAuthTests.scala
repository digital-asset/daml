// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

trait AdminServiceCallAuthTests extends ServiceCallAuthTests {

  private val signedIncorrectly = Option(toHeader(adminToken, UUID.randomUUID.toString))

  it should "deny calls with an invalid signature" in {
    expectUnauthenticated(serviceCallWithToken(signedIncorrectly))
  }
  it should "deny calls with an expired admin token" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "deny calls with a read-only token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with a read/write token" in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "allow calls with explicitly non-expired admin token" in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }
  it should "allow calls with admin token without expiration" in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }

  it should "allow calls with the correct ledger ID" in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomParticipantId))
  }

}
