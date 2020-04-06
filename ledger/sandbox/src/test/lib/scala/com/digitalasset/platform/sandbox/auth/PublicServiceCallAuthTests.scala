// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

trait PublicServiceCallAuthTests extends ServiceCallAuthTests {

  it should "deny calls with an expired read-only token" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsRandomPartyExpired))
  }
  it should "allow calls with explicitly non-expired read-only token" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyExpiresTomorrow))
  }
  it should "allow calls with read-only token without expiration" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomParty))
  }

  it should "deny calls with an expired read/write token" in {
    expectUnauthenticated(serviceCallWithToken(canActAsRandomPartyExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" in {
    expectSuccess(serviceCallWithToken(canActAsRandomPartyExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" in {
    expectSuccess(serviceCallWithToken(canActAsRandomParty))
  }

  it should "deny calls with an expired admin token" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "allow calls with explicitly non-expired admin token" in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }
  it should "allow calls with admin token without expiration" in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }

  it should "allow calls with the correct ledger ID" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomParticipantId))
  }
}
