// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

final class VariantConstructorClassSpec extends AnyFlatSpec with Matchers {

  behavior of "VariantConstructorClass.lowerCaseFieldName"

  it should "lower case first character" in {
    VariantConstructorClass.lowerCaseFieldName("ABcdEF") shouldEqual "aBcdEFValue"
  }

  it should "lower case single character" in {
    VariantConstructorClass.lowerCaseFieldName("A") shouldEqual "aValue"
  }

  it should "not fail on empty input" in {
    VariantConstructorClass.lowerCaseFieldName("") shouldEqual "Value"
  }

}
