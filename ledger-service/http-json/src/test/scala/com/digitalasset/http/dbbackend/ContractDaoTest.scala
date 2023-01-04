// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package dbbackend

import com.daml.scalatest.WordSpecCheckLaws
import scalaz.{Order, Traverse}
import scalaz.scalacheck.{ScalazProperties => ZP}
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
    with ScalaCheckDrivenPropertyChecks
    with WordSpecCheckLaws {
  import ContractDaoTest._

  "minimumViableOffsets" should {
    import ContractDao.minimumViableOffsets

    type Unsynced[Off] = Map[Byte, Map[Byte, Off]]
    val UF: Traverse[Unsynced] = {
      val one = Traverse[Map[Byte, *]]
      one compose one
    }
    val queriedParties = (0 to 5).view.map(_.toByte).toSet
    def everyParty[Off](off: Off) = queriedParties.view.map((_, off)).toMap
    def mvo[TpId, Off: Order](expectedOffset: Off, unsynced: Map[TpId, Map[Byte, Off]]) =
      minimumViableOffsets(queriedParties, identity[TpId], expectedOffset, unsynced)

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

    "require update for a template ID that is caught up in a different party" in {
      mvo(1, Map(0 -> Map((2: Byte) -> 1, (3: Byte) -> 2))) should ===(Some((2, Set(0))))
    }

    "not require update for lagging, but unqueried, party" in {
      mvo(3, Map(0 -> Map((-4: Byte) -> 1))) should ===(None)
    }

    "require update for ahead, albeit unqueried, party" in {
      mvo(3, Map(0 -> Map((-4: Byte) -> 4))) should ===(Some((4, Set(0))))
    }

    "report desync, but no updates, if consistent" in {
      mvo(3, Map(0 -> everyParty(5), 1 -> everyParty(5))) should ===(
        Some((5, Set.empty))
      )
    }

    "check lag across template IDs" in {
      mvo(3, Map(0 -> Map((2: Byte) -> 3), 1 -> everyParty(5))) should ===(Some((5, Set(0))))
    }

    "update an expected-offset template ID when another template ID is ahead" in {
      mvo(3, Map(0 -> Map.empty[Byte, Int], 1 -> everyParty(5))) should ===(Some((5, Set(0))))
    }
  }

  "Lagginess" should {
    import ContractDao.Lagginess
    checkLaws(ZP.semigroup.laws[Lagginess[Byte, Byte]])
  }
}

object ContractDaoTest {
  import org.scalacheck.Arbitrary, Arbitrary.arbitrary
  import ContractDao.Lagginess

  implicit def `arb Lagginess`[TpId: Arbitrary, Off: Arbitrary]: Arbitrary[Lagginess[TpId, Off]] =
    Arbitrary(arbitrary[(Set[TpId], Set[TpId], Off)].map((Lagginess[TpId, Off] _).tupled))
}
