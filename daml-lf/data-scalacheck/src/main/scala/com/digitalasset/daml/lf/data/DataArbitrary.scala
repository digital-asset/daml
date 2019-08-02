// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import ImmArray.ImmArraySeq
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary

object DataArbitrary {
  implicit def arbFrontStack[A: Arbitrary]: Arbitrary[FrontStack[A]] =
    Arbitrary(
      arbitrary[Vector[(A, Option[ImmArray[A]])]]
        .map(_.foldRight(FrontStack.empty[A]) {
          case ((a, oia), acc) => oia.fold(a +: acc)(ia => (ia slowCons a) ++: acc)
        }))

  implicit def arbImmArray[A: Arbitrary]: Arbitrary[ImmArray[A]] =
    Arbitrary {
      for {
        raw <- arbitrary[Seq[A]]
        min <- Gen.choose(0, 0 max (raw.size - 1))
        max <- Gen.choose(min, raw.size)
      } yield if (min >= max) ImmArray(Seq()) else ImmArray(raw).strictSlice(min, max)
    }

  implicit def arbImmArraySeq[A: Arbitrary]: Arbitrary[ImmArraySeq[A]] =
    Arbitrary(arbitrary[ImmArray[A]] map (_.toSeq))
}
