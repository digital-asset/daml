// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package dbbackend

import scalaz.Order
import scalaz.std.anyVal._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractDaoTest
    extends AnyWordSpec
    with Matchers
    with OptionValues
    with ScalaCheckDrivenPropertyChecks {
  "minimumViableOffsets" should {
    import ContractDao.minimumViableOffsets

    type Unsynced[Off] = Map[Byte, Map[Byte, Off]]
    def mapUnsynced[A, B](fa: Unsynced[A])(f: A => B) =
      fa transform ((_, m) => m transform ((_, o) => f(o)))
    def mvo[Off: Order](expectedOffset: Off, unsynced: Unsynced[Off]) =
      minimumViableOffsets((_: Byte) < 0, identity[Byte], expectedOffset, unsynced)

    "return all OK if all offsets match" in forAll { unsynced: Unsynced[Unit] =>
      val allSame = mapUnsynced(unsynced)(_ => 0)
      mvo(0, allSame) shouldBe None
    }

    "use the larger offset" in forAll { (a: Byte, b: Byte, unsynced: Unsynced[Boolean]) =>
      whenever(unsynced.valuesIterator.flatMap(_.valuesIterator).toSet == Set(true, false)) {
        val (min, max) = if (a < b) (a, b) else if (a == b) (0: Byte, 1: Byte) else (b, a)
        val eitherOr = mapUnsynced(unsynced)(b => if (b) min else max)
        mvo(min, eitherOr).value._1 should ===(max)
      }
    }
  }
}
