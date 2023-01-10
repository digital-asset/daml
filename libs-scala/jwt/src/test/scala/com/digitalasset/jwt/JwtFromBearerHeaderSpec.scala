// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JwtFromBearerHeaderSpec extends AnyFlatSpec with Matchers {

  it should "produce an error in case of empty string as header" in {
    JwtFromBearerHeader("") shouldBe Left(
      Error(Symbol("JwtFromBearerHeader"), "Authorization header does not use Bearer format")
    )
  }

  it should "produce an error in case of missing Bearer header" in {
    JwtFromBearerHeader("Bearer") shouldBe Left(
      Error(Symbol("JwtFromBearerHeader"), "Authorization header does not use Bearer format")
    )

    JwtFromBearerHeader("Bearer ") shouldBe Left(
      Error(Symbol("JwtFromBearerHeader"), "Authorization header does not use Bearer format")
    )
  }

  it should "extract valid token from the header" in {
    JwtFromBearerHeader("Bearer 123") shouldBe Right("123")
  }

}
