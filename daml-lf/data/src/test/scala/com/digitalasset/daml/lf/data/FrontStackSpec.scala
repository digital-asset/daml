// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{Matchers, WordSpec}
import scalaz.scalacheck.ScalazProperties
import scalaz.std.anyVal._

class FrontStackSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks
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

    "more than 2 elements are provided" should {
      "behave the same as prepend" in forAll { (x: Int, y: Int, z: Int, rest: Seq[Int]) =>
        FrontStack(x, y, z, rest: _*) should ===(
          ImmArray(Seq(x, y, z) ++ rest) ++: FrontStack.empty)
      }
    }

    "a sequence of elements is provided" should {
      "behave the same as prepend" in forAll { xs: Seq[Int] =>
        FrontStack(xs) should ===(ImmArray(xs) ++: FrontStack.empty)
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
      fs.toImmArray should ===(fs.iterator.to[ImmArray])
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
  }

  "Equal instance" should {
    checkLaws(ScalazProperties.equal.laws[FrontStack[Unnatural[Int]]])
  }
}
