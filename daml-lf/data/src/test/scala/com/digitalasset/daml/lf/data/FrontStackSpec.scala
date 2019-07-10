// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.{WordSpec, Matchers}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}

class FrontStackSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks with TableDrivenPropertyChecks {
  import ImmArrayTest._, FrontStackSpec._

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
    "be tracked accurately during building" in forAll {fs: FrontStack[Int] =>
      fs.length should ===(fs.iterator.length)
    }
  }

  "slowApply" should {
    "throw when out of bounds" in forAll {fs: FrontStack[Int] =>
      an[IndexOutOfBoundsException] should be thrownBy(fs.slowApply(-1))
      an[IndexOutOfBoundsException] should be thrownBy(fs.slowApply(fs.length))
    }

    "preserve Seq's apply" in forAll {fs: FrontStack[Int] =>
      val expected = Table(
        ("value", "index"),
        fs.toImmArray.toSeq.zipWithIndex:_*
      )
      forEvery(expected) {(value, index) =>
        fs.slowApply(index) should ===(value)
      }
    }
  }
}

object FrontStackSpec {
  import org.scalacheck.Arbitrary
  import ImmArrayTest._

  implicit def arbFrontStack[A: Arbitrary]: Arbitrary[FrontStack[A]] =
    Arbitrary(Arbitrary.arbitrary[Vector[(A, Option[ImmArray[A]])]].map(_.foldRight(FrontStack.empty[A]){
      case ((a, oia), acc) => oia.fold(a +: acc)(ia => (ia slowCons a) ++: acc)
    }))
}
