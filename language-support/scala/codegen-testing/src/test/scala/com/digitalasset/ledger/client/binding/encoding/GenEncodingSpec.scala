// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.client.binding.{Primitive => P}
import org.scalatest.Succeeded
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.Show

class GenEncodingSpec extends AnyWordSpec with ScalaCheckDrivenPropertyChecks {
  import ShowEncoding.Implicits._

  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 10000)

  "P.Text arbitrary Gen should not generate \\u0000, PostgreSQL does not like it" in forAll(
    GenEncoding.postgresSafe.primitive.valueText
  ) { text: P.Text =>
    val show: Show[P.Text] = implicitly
    if (text.forall(_ != '\u0000')) Succeeded
    else
      fail(
        s"P.Text generator produced a string with unexpected character, text: ${show.show(text)}"
      )
  }

}
