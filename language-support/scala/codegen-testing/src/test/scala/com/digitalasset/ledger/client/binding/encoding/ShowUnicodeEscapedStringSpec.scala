// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.client.binding.encoding.EncodingUtil.normalize
import org.apache.commons.text.StringEscapeUtils
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.Cord

class ShowUnicodeEscapedStringSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 10000)

  "should unicode-escape all non-ascii chars in the format that can compile back to original string" in {
    "scho\u0308n" should !==("schön")
    normalize("scho\u0308n") should ===(normalize("schön"))

    println(ShowUnicodeEscapedString.show("scho\u0308n"))
    ShowUnicodeEscapedString.show("scho\u0308n").toString should ===("\"scho\\u0308n\"")

    println(ShowUnicodeEscapedString.show("schön"))
    ShowUnicodeEscapedString.show("schön").toString should ===("\"sch\\u00F6n\"")
    "sch\u00F6n" should ===("schön")
    "\u00F6" should ===("ö")
  }

  "normalizing unicode string multiple times does not change it" in {
    "scho\u0308n" should !==("schön")
    normalize("scho\u0308n") should ===(normalize("schön"))
    normalize(normalize("scho\u0308n")) should ===(normalize("schön"))
    normalize("scho\u0308n") should ===(normalize(normalize("schön")))
  }

  "ASCII slash should be unicode escaped" in {
    ShowUnicodeEscapedString.show("\\").toString.getBytes should ===("\"\\u005C\"".getBytes)
  }

  "unicode escaped string can be interpreted back to original string one example" in {
    testUnicodeEscapedStringCanBeUnescapedBackToOriginalString("scho\u0308n")
  }

  "unicode escaped zero can be interpreted back" in {
    testUnicodeEscapedStringCanBeUnescapedBackToOriginalString("\u0000")
  }

  "backslash followed by zero caused some issues" in {
    testUnicodeEscapedStringCanBeUnescapedBackToOriginalString("\\\u0000")
  }

  "any unicode escaped string can be interpreted back to original string" in forAll { s: String =>
    testUnicodeEscapedStringCanBeUnescapedBackToOriginalString(s)
  }

  private def testUnicodeEscapedStringCanBeUnescapedBackToOriginalString(s0: String): Assertion = {
    val s1: Cord = ShowUnicodeEscapedString.show(s0)
    val s2: String = StringEscapeUtils.unescapeJava(removeWrappingQuotes(s1.toString))
    s2.getBytes should ===(s0.getBytes)
  }

  private def removeWrappingQuotes(s: String): String = {
    require(s.length >= 2)
    s.substring(1, s.length - 1)
  }
}
