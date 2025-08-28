// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.language.LanguageMajorVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractValidationSpec extends AnyWordSpec with Matchers {

  "BuildAndValidate" should {
    "tbc" in {
      ContractValidation(Engine.DevEngine(LanguageMajorVersion.V2))
      succeed
    }
  }

}
