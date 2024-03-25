// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import org.scalacheck.util.Buildable
import scalaz.{@@, Tag}

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

  private[this] sealed trait APS
  private[this] implicit val aaps: Arbitrary[String @@ APS] =
    Tag.subst(Arbitrary(Gen.asciiPrintableStr))
  implicit def `arb SortedLookupList`[A: Arbitrary]: Arbitrary[SortedLookupList[A]] =
    Arbitrary(
      Tag
        .unsubst[String, Lambda[k => Gen[Map[k, A]]], APS](arbitrary[Map[String @@ APS, A]])
        .map(SortedLookupList(_))
    )

  // The default collection instances don't make smaller-sized elements.
  sealed trait Div3 // XXX in scala 2.13 consider a Nat tparam
  private[this] def div3[T](g: Gen[T]): Gen[T] =
    Gen sized (n => Gen.resize(n / 3, g))

  implicit def `arb container1 Div3`[C, T](implicit
      t: C => Iterable[T],
      b: Buildable[T, C],
      a: Arbitrary[T],
  ): Arbitrary[C @@ Div3] =
    Tag subst Arbitrary(Gen buildableOf div3(a.arbitrary))

  implicit def `arb SortedLookupList Div3`[A: Arbitrary]: Arbitrary[SortedLookupList[A] @@ Div3] =
    Tag subst `arb SortedLookupList`(Arbitrary(div3(arbitrary[A])))
}
