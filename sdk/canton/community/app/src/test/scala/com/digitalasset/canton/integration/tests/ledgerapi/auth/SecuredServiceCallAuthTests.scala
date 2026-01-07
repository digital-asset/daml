// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*

trait SecuredServiceCallAuthTests extends ServiceCallAuthTests {

  serviceCallName should {
    "deny unauthenticated calls" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present a JWT")
    ) in { implicit env =>
      import env.*
      expectUnauthenticated(serviceCall(noToken))
    }
  }
}
