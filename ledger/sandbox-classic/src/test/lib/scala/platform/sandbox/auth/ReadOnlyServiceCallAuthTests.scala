// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import org.scalatest.Assertion

import scala.concurrent.Future

trait ReadOnlyServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  /**
    * Allows to override what is regarded as a successful response, e.g. lookup queries for
    * commands can return a NOT_FOUND, which is fine because the result is not PERMISSION_DENIED
    */
  def successfulBehavior: Future[Any] => Future[Assertion] = expectSuccess(_: Future[Any])

  it should "deny calls with an expired read-only token" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read-only token" in {
    successfulBehavior(serviceCallWithToken(canReadAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read-only token without expiration" in {
    successfulBehavior(serviceCallWithToken(canReadAsMainActor))
  }

  it should "deny calls with an expired read/write token" in {
    expectUnauthenticated(serviceCallWithToken(canActAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" in {
    successfulBehavior(serviceCallWithToken(canActAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" in {
    successfulBehavior(serviceCallWithToken(canActAsMainActor))
  }

  it should "allow calls with the correct ledger ID" in {
    successfulBehavior(serviceCallWithToken(canReadAsMainActorActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActorRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    successfulBehavior(serviceCallWithToken(canReadAsMainActorActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActorRandomParticipantId))
  }
}
