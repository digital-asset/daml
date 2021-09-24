// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package dbbackend

import scalaz.{Order, Traverse}
import scalaz.std.anyVal._
import scalaz.std.map._
import scalaz.std.set._
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
    val UF: Traverse[Unsynced] = {
      val one = Traverse[Map[Byte, *]]
      one compose one
    }
    def mvo[Off: Order](expectedOffset: Off, unsynced: Unsynced[Off]) =
      minimumViableOffsets((_: Byte) < 0, identity[Byte], expectedOffset, unsynced)

    "return all OK if all offsets match" in forAll { unsynced: Unsynced[Unit] =>
      val allSame = UF.map(unsynced)(_ => 0)
      mvo(0, allSame) shouldBe None
    }

    "use the larger offset" in forAll { (a: Byte, b: Byte, unsynced: Unsynced[Boolean]) =>
      whenever(UF.foldMap(unsynced)(Set(_)) == Set(true, false)) {
        val (min, max) = if (a < b) (a, b) else if (a == b) (0: Byte, 1: Byte) else (b, a)
        val eitherOr = UF.map(unsynced)(b => if (b) min else max)
        mvo(min, eitherOr).value._1 should ===(max)
      }
    }
  }
}
