// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

trait ReadWriteServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  it should "deny calls with an expired read/write token" in {
    expectUnauthenticated(serviceCallWithToken(canActAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" in {
    expectSuccess(serviceCallWithToken(canActAsMainActor))
  }

  it should "deny calls with explicitly non-expired read-only token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActorExpiresTomorrow))
  }
  it should "deny calls with read-only token without expiration" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActor))
  }

  it should "allow calls with the correct ledger ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomParticipantId))
  }
}
