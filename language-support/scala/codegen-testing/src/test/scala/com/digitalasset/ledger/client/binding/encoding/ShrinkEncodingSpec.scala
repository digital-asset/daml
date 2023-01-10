// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.client.binding.{Primitive => P}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ShrinkEncodingSpec extends AnyWordSpec with ScalaCheckDrivenPropertyChecks {

  "ShrinkEncoding.primitive.valueParty should not generate \\u0000, PostgreSQL does not like them" in forAll(
    GenEncoding.primitive.valueParty
  ) { p: P.Party =>
    import EqualityEncoding.Implicits._
    import ShowEncoding.Implicits._
    import com.daml.scalatest.CustomMatcher._
    import scalaz.std.AllInstances._

    val list: List[P.Party] = ShrinkEncoding.primitive.valueParty.shrink(p).toList
    list.forall { a: P.Party =>
      val str: String = P.Party.unwrap(a)
      str.forall(_.isLetterOrDigit)
    } should_=== true

    val lastP: Option[P.Party] = list.lastOption
    lastP should_=== Some(P.Party(""))
  }

  "GenEncoding.primitive.valueParty should not generate \\u0000,  PostgreSQL does not like them" in forAll(
    GenEncoding.primitive.valueParty
  ) { p: P.Party =>
    val str: String = P.Party.unwrap(p)
    if (str.contains("\u0000")) {
      fail(s"Party contains illegal chars: ${ShowEncoding.primitive.valueParty.show(p)}")
    }
  }
}
