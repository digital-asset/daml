// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.time.Duration
import java.util.UUID

trait ServiceCallWithMainActorAuthTests extends ServiceCallAuthTests {

  protected val mainActor: String = UUID.randomUUID.toString

  private val signedIncorrectly =
    Option(toHeader(readWriteToken(mainActor), UUID.randomUUID.toString))

  it should "deny calls authorized to read/write as the wrong party" in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "deny calls authorized to read-only as the wrong party" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with an invalid signature" in {
    expectPermissionDenied(serviceCallWithToken(signedIncorrectly))
  }

  protected val canReadAsMainActor =
    Option(toHeader(readOnlyToken(mainActor)))
  protected val canReadAsMainActorExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(mainActor))))
  protected val canReadAsMainActorExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readOnlyToken(mainActor))))

  protected val canActAsMainActor =
    Option(toHeader(readWriteToken(mainActor)))
  protected val canActAsMainActorExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readWriteToken(mainActor))))
  protected val canActAsMainActorExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readWriteToken(mainActor))))

}
