// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

trait UnsecuredServiceCallAuthTests extends ServiceCallAuthTests {
  behavior of serviceCallName

  it should "allow unauthenticated calls" taggedAs securityAsset.setHappyCase(
    "Ledger API client can make a call without authentication"
  ) in {
    expectSuccess(serviceCallWithToken(None))
  }
}
