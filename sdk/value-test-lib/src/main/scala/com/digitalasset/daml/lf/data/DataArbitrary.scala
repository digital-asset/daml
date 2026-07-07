// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary

object DataArbitrary {
  implicit def `arb FrontStack`[A: Arbitrary]: Arbitrary[FrontStack[A]] =
    Arbitrary(
      arbitrary[Vector[(A, Option[ImmArray[A]])]]
        .map(_.foldRight(FrontStack.empty[A]) { case ((a, oia), acc) =>
          oia.fold(a +: acc)(ia => (ia slowCons a) ++: acc)
        })
    )

  implicit def `arb ImmArray`[A: Arbitrary]: Arbitrary[ImmArray[A]] =
    Arbitrary {
      for {
        raw <- arbitrary[Seq[A]]
        min <- Gen.choose(0, 0 max (raw.size - 1))
        max <- Gen.choose(min, raw.size)
      } yield if (min >= max) ImmArray.Empty else raw.to(ImmArray).strictSlice(min, max)
    }

  implicit def `arb ImmArraySeq`[A: Arbitrary]: Arbitrary[ImmArray.ImmArraySeq[A]] =
    Arbitrary(arbitrary[ImmArray[A]] map (_.toSeq))

  implicit def `arb SortedLookupList`[A: Arbitrary]: Arbitrary[SortedLookupList[A]] =
    Arbitrary(
      Gen
        .mapOf(Gen.zip(Gen.asciiPrintableStr, arbitrary[A]))
        .map(SortedLookupList.from(_))
    )

  // The default collection instances don't make smaller-sized elements.
  private[this] def div3[T](g: Gen[T]): Gen[T] =
    Gen sized (n => Gen.resize(n / 3, g))
}
