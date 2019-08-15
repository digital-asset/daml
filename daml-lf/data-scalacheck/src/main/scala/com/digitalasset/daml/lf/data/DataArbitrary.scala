// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import ImmArray.ImmArraySeq
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import scalaz.{@@, Tag}

object DataArbitrary {
  implicit def `arb FrontStack`[A: Arbitrary]: Arbitrary[FrontStack[A]] =
    Arbitrary(
      arbitrary[Vector[(A, Option[ImmArray[A]])]]
        .map(_.foldRight(FrontStack.empty[A]) {
          case ((a, oia), acc) => oia.fold(a +: acc)(ia => (ia slowCons a) ++: acc)
        }))

  implicit def `arb ImmArray`[A: Arbitrary]: Arbitrary[ImmArray[A]] =
    Arbitrary {
      for {
        raw <- arbitrary[Seq[A]]
        min <- Gen.choose(0, 0 max (raw.size - 1))
        max <- Gen.choose(min, raw.size)
      } yield if (min >= max) ImmArray(Seq()) else ImmArray(raw).strictSlice(min, max)
    }

  implicit def `arb ImmArraySeq`[A: Arbitrary]: Arbitrary[ImmArraySeq[A]] =
    Arbitrary(arbitrary[ImmArray[A]] map (_.toSeq))

  private[this] sealed trait APS
  private[this] implicit val aaps: Arbitrary[String @@ APS] =
    Tag.subst(Arbitrary(Gen.asciiPrintableStr))
  implicit def `arb SortedLookupList`[A: Arbitrary]: Arbitrary[SortedLookupList[A]] =
    Arbitrary(
      Tag
        .unsubst[String, Lambda[k => Gen[Map[k, A]]], APS](arbitrary[Map[String @@ APS, A]])
        .map(SortedLookupList(_)))
}
