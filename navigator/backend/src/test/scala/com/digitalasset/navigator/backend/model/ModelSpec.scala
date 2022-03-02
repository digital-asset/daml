// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ModelSpec extends AnyWordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  val templateId = C.ref0

  "Navigator data model" when {

    "converting Daml-LF identifiers to API and back" should {
      val result = templateId.asApi.asDaml

      "not change the value" in {
        result shouldBe templateId
      }
    }

    "converting Daml-LF identifiers to opaque string and parsing back" should {
      val result = parseOpaqueIdentifier(templateId.asOpaqueString)

      "not change the value" in {
        result shouldBe Some(templateId)
      }
    }
  }
}
