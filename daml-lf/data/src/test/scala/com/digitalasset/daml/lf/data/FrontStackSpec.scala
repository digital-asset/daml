// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.scalatest.{Unnatural, WordSpecCheckLaws}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.scalacheck.ScalazProperties
import scalaz.std.anyVal._

class FrontStackSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with WordSpecCheckLaws {

  import DataArbitrary._

  "apply" when {
    "1 element is provided" should {
      "behave the same as prepend" in forAll { x: Int =>
        FrontStack(x) should ===(x +: FrontStack.empty)
      }
    }

    "2 elements are provided" should {
      "behave the same as prepend" in forAll { (x: Int, y: Int) =>
        FrontStack(x, y) should ===(x +: y +: FrontStack.empty)
      }
    }
  }

  "++:" should {
    "yield equal results to +:" in forAll { (ia: ImmArray[Int], fs: FrontStack[Int]) =>
      (ia ++: fs) should ===(ia.toSeq.foldRight(fs)(_ +: _))
    }
  }

  "toImmArray" should {
    "yield same elements as iterator" in forAll { fs: FrontStack[Int] =>
      fs.toImmArray should ===(fs.iterator.to(ImmArray))
    }
  }

  "length" should {
    "be tracked accurately during building" in forAll { fs: FrontStack[Int] =>
      fs.length should ===(fs.iterator.length)
    }
  }

  "slowApply" should {
    "throw when out of bounds" in forAll { fs: FrontStack[Int] =>
      an[IndexOutOfBoundsException] should be thrownBy fs.slowApply(-1)
      an[IndexOutOfBoundsException] should be thrownBy fs.slowApply(fs.length)
    }

    "preserve Seq's apply" in forAll { fs: FrontStack[Int] =>
      val expected = Table(
        ("value", "index"),
        fs.toImmArray.toSeq.zipWithIndex: _*
      )
      forEvery(expected) { (value, index) =>
        fs.slowApply(index) should ===(value)
      }
    }
  }

  "toBackStack" should {
    "be retracted by toFrontStack" in forAll { fs: FrontStack[Int] =>
      fs.toBackStack.toFrontStack should ===(fs)
    }
  }

  "Traverse instance" should {
    checkLaws(ScalazProperties.traverse.laws[FrontStack])

    "reconstruct itself with foldRight" in forAll { fs: FrontStack[Int] =>
      scalaz.Foldable[FrontStack].foldRight(fs, FrontStack.empty[Int])(_ +: _) should ===(fs)
    }
  }

  "Equal instance" should {
    checkLaws(ScalazProperties.equal.laws[FrontStack[Unnatural[Int]]])
  }
}
