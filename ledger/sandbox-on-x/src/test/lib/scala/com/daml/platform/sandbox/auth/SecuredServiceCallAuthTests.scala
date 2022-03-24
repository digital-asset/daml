// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

trait SecuredServiceCallAuthTests extends ServiceCallAuthTests {
  behavior of serviceCallName

  val securityAsset: SecurityTest =
    SecurityTest(property = Authorization, asset = serviceCallName)

  def attack(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse to connect the user to $serviceCallName",
  )

  it should "deny unauthenticated calls" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a missing token")
  ) in {
    expectUnauthenticated(serviceCallWithToken(None))
  }
}
