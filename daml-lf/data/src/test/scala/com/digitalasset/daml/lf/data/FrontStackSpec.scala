// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}

import scalaz.scalacheck.ScalazProperties
import scalaz.std.anyVal._

class FrontStackSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks
    with WordSpecCheckLaws {
  import DataArbitrary._

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
      an[IndexOutOfBoundsException] should be thrownBy (fs.slowApply(-1))
      an[IndexOutOfBoundsException] should be thrownBy (fs.slowApply(fs.length))
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
