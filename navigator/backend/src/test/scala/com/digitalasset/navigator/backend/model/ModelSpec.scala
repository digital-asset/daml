// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

import org.scalatest.{Matchers, WordSpec}

class ModelSpec extends WordSpec with Matchers {
  import com.digitalasset.navigator.{DamlConstants => C}

  val templateId = C.ref0

  "Navigator data model" when {

    "converting DAML-LF identifiers to API and back" should {
      val result = templateId.asApi.asDaml

      "not change the value" in {
        result shouldBe templateId
      }
    }

    "converting DAML-LF identifiers to opaque string and parsing back" should {
      val result = parseOpaqueIdentifier(templateId.asOpaqueString)

      "not change the value" in {
        result shouldBe Some(templateId)
      }
    }
  }
}
