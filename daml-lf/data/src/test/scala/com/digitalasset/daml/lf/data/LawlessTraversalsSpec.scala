// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ImmArray.ImmArraySeq

import org.scalatest.{Inside, Matchers, WordSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LawlessTraversalsSpec
    extends WordSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {
  import LawlessTraversals._

  "traverseEitherStrictly" should {
    "satisfy identity, elementwise" in forAll { xs: Seq[Int] =>
      xs traverseEitherStrictly (Right(_)) should ===(Right(xs))
    }

    "preserve class if the implementation bothered to set it up" in {
      val classySeqs = Seq[Seq[Int]](List(1), Vector(2), ImmArraySeq(3))
      // we need to use patmat, not == or shouldBe, because patmat is stricter
      inside(classySeqs map (_ traverseEitherStrictly (Right(_)))) {
        case Seq(_, Right(List(_)), _) => fail("lists are not vectors")
        case Seq(Right(List(1)), Right(Vector(2)), Right(ImmArraySeq(3))) =>
      }
    }
  }
}
