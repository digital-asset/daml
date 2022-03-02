// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class LawlessTraversalsSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {
  import LawlessTraversals._

  "traverseEitherStrictly" should {
    "satisfy identity, elementwise" in forAll { xs: Seq[Int] =>
      xs traverseEitherStrictly (Right(_)) should ===(Right(xs))
    }

    "preserve class if the implementation bothered to set it up" in {
      val classySeqs = Seq[Seq[Int]](List(1), Vector(2), ImmArray.ImmArraySeq(3))
      // we need to use patmat, not == or shouldBe, because patmat is stricter
      inside(classySeqs map (_ traverseEitherStrictly (Right(_)))) {
        case Seq(_, Right(List(_)), _) => fail("lists are not vectors")
        case Seq(Right(List(1)), Right(Vector(2)), Right(ImmArray.ImmArraySeq(3))) =>
      }
    }
  }
}
