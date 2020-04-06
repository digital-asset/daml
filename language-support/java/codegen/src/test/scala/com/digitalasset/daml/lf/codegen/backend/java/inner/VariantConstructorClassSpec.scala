// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import org.scalatest.{FlatSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class VariantConstructorClassSpec extends FlatSpec with Matchers {

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
