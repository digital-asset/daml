// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.{Equal, Show}

class CustomMatcherSpec extends AnyWordSpec with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 10000)

  "make sure it works comparing ints" in {
    import com.daml.scalatest.CustomMatcher._
    import scalaz.std.anyVal._

    CustomMatcherOps(10) should_=== 10
    CustomMatcherOps(10) should_=/= 11

    10 should_=== 10
    10 should_=/= 11
  }

  case class Dummy(a: String, b: Int, c: BigDecimal)

  lazy val genDummy: Gen[Dummy] = for {
    a <- arbitrary[String]
    b <- arbitrary[Int]
    c <- arbitrary[BigDecimal]
  } yield Dummy(a, b, c)

  lazy val genPairOfNonEqualDummies: Gen[(Dummy, Dummy)] = {
    def genSetOf2: Gen[Set[Dummy]] =
      Gen.buildableOfN[Set[Dummy], Dummy](2, genDummy).filter(_.size == 2)

    genSetOf2.map(_.toSeq).map {
      case Seq(a, b) => (a, b)
      case a @ _ => sys.error(s"Should never happen: $a")
    }
  }

  implicit val dummyEqual: Equal[Dummy] = Equal.equalA

  implicit val dummyShow: Show[Dummy] = Show.showA

  "make sure it works comparing case classes with custom Show and Equal" in forAll(
    genPairOfNonEqualDummies
  ) { case (a, b) =>
    import com.daml.scalatest.CustomMatcher._

    a should_=== a
    a should_=== a.copy()

    a should_=/= b
  }
}
