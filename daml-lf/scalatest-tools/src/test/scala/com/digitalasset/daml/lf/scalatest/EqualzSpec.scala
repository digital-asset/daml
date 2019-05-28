// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import org.scalatest.{WordSpec, Matchers}
import scalaz.std.anyVal._
import scalaz.std.option._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class EqualzSpec extends WordSpec with Matchers {
  import Equalz._

  "equalz" should {
    "accept left <: right" in {
      Some(42) shouldx equalz(some(42))
      Some(42) shouldNotx equalz(none[Int])
      None shouldx equalz(none[Int])
    }

    "accept left >: right" in {
      some(42) shouldx equalz(Some(42))
      none[Int] shouldNotx equalz(Some(84))
    }

    "reject mismatched types" in {
      "some(42) shouldx equalz(42)" shouldNot typeCheck
      "42 shouldx equalz(some(42))" shouldNot typeCheck
    }

    "disallow cheating by widening the right" in {
      42 shouldNot equal(Some(42): Any)
      42 should !==(Some(42): Any)
      "42 shouldNotx equalz(Some(42): Any)" shouldNot typeCheck
    }

    "reject missing Equal typeclass instances" in {
      "None shouldx equalz(None)" shouldNot typeCheck
      object Blah
      Blah
      "Blah shouldx equalz(Blah)" shouldNot typeCheck
    }
  }
}
