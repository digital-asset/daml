// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.interp.testing

import com.digitalasset.daml.lf.data.Numeric
import com.digitalasset.daml.lf.speedy.{SBuiltin, SValue}
import com.digitalasset.daml.lf.speedy.SValue._
import org.scalatest.{Matchers, WordSpec}
import java.util.ArrayList

class ToTextTest extends WordSpec with Matchers {
  def litToText(lit: SValue): String = {
    val xs = new ArrayList[SValue]()
    xs.add(lit)
    SBuiltin.SBToText.litToText(xs).asInstanceOf[SText].value
  }

  "toString" should {
    "Decimal" in {
      litToText(SNumeric(Numeric.assertFromString("123.456000000"))) shouldBe "123.456"
      litToText(SNumeric(Numeric.assertFromString("123.000000000"))) shouldBe "123.0"
      litToText(SNumeric(Numeric.assertFromString("0.100000000"))) shouldBe "0.1"
    }

    "Text" in {
      litToText(SText("foo\"bar")) shouldBe "foo\"bar"
    }
  }
}
