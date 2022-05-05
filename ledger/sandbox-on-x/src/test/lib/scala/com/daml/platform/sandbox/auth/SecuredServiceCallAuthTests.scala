// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

trait SecuredServiceCallAuthTests extends ServiceCallAuthTests {
  behavior of serviceCallName

  it should "deny unauthenticated calls" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Do not present a JWT")
  ) in {
    expectUnauthenticated(serviceCallWithToken(None))
  }
}
