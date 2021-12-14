// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future

trait ReadWriteServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  def serviceCallWithoutApplicationId(token: Option[String]): Future[Any]

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
  it should "allow calls with the correct application ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualApplicationId))
  }
  it should "deny calls with a random application ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomApplicationId))
  }
  it should "allow calls with an empty application ID for a token with an application id" in {
    expectSuccess(serviceCallWithoutApplicationId(canActAsMainActorActualApplicationId))
  }
  it should "deny calls with an empty application ID for a token without an application id" in {
    expectInvalidArgument(serviceCallWithoutApplicationId(canActAsMainActor))
  }
}
