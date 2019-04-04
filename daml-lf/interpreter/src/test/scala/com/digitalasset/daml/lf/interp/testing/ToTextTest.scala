// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.interp.testing

import com.digitalasset.daml.lf.data.{Decimal}
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
      litToText(SDecimal(Decimal.fromString("123.4560000").toOption.get)) shouldBe "123.456"
      litToText(SDecimal(Decimal.fromString("123.0000000").toOption.get)) shouldBe "123.0"
    }

    "Text" in {
      litToText(SText("foo\"bar")) shouldBe "foo\"bar"
    }
  }
}
