// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

class HandshakeValidatorTest extends AsyncWordSpec with BaseTest {
  private val validator = new HandshakeValidator(testedProtocolVersion, loggerFactory)

  "HandshakeValidator" should {
    "happy path" in {

      val tested = testedProtocolVersion.toString

      // success because both support tested protocol version
      validator
        .clientIsCompatible(Seq(tested), minClientVersionP = None)
        .value shouldBe ()
    }

    "succeed even if one client version is unknown to the server" in {
      val unknownProtocolVersion = "42000"
      val tested = testedProtocolVersion.toString

      // success because both support tested protocol version
      validator
        .clientIsCompatible(Seq(tested, unknownProtocolVersion), minClientVersionP = None)
        .value shouldBe ()

      // failure: no common pv
      validator
        .clientIsCompatible(Seq(unknownProtocolVersion), minClientVersionP = None)
        .left
        .value shouldBe a[String]
    }

    "take minimum protocol version into account" in {
      if (testedProtocolVersion.isStable) {
        val tested = testedProtocolVersion.toString

        // testedProtocolVersion is lower than minimum protocol version
        validator
          .clientIsCompatible(Seq(tested), minClientVersionP = Some("42"))
          .left
          .value shouldBe a[String]
      } else succeed
    }
  }
}
