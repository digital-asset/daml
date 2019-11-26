// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

trait AdminServiceCallAuthTests extends ServiceCallAuthTests {

  private val signedIncorrectly = Option(toHeader(adminToken, UUID.randomUUID.toString))

  it should "deny calls with an invalid signature" in {
    expectPermissionDenied(serviceCallWithToken(signedIncorrectly))
  }
  it should "deny calls with an expired admin token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminExpired))
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

}
