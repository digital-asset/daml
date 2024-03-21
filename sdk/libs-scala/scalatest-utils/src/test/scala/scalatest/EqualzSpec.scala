// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.std.anyVal._
import scalaz.std.option._

class EqualzSpec extends AnyWordSpec with Matchers {
  import Equalz._

  "equalz" should {
    "accept left ~ right" in {
      some(42) should equalz(some(42))
      some(42) shouldNot equalz(none[Int])
      none[Int] should equalz(none[Int])
    }

    "accept left <: right" in {
      Some(42) should equalz(some(42))
      Some(42) shouldNot equalz(none[Int])
      None should equalz(none[Int])
    }

    "accept left >: right, with alternative syntax" in {
      some(42) shouldx equalz(Some(42))
      none[Int] shouldNotx equalz(Some(84))
    }

    "reject mismatched types" in {
      "some(42) should equalz(42)" shouldNot typeCheck
      "42 should equalz(some(42))" shouldNot typeCheck
    }

    "disallow cheating by widening the right" in {
      42 shouldNot equal(Some(42): Any)
      42 should !==(Some(42): Any)
      "42 shouldNot equalz(Some(42): Any)" shouldNot typeCheck
    }

    "reject missing Equal typeclass instances" in {
      "None should equalz(None)" shouldNot typeCheck
      object Blah
      Blah
      "Blah should equalz(Blah)" shouldNot typeCheck
    }
  }
}
