// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty
package catsinstances

import com.daml.scalatest.WordSpecCheckLaws
import NonEmptyReturningOps._

import cats.{Foldable, Reducible, Traverse}
import cats.kernel.Eq
import org.scalacheck.Arbitrary, Arbitrary.arbitrary
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class CatsInstancesSpec extends AnyWordSpec with Matchers with WordSpecCheckLaws {
  import CatsInstancesSpec._

  "Foldable" should {
    "be preferred if Reducible isn't need" in {
      import cats.instances.list._
      Foldable[NonEmptyF[List, *]].getClass should be theSameInstanceAs Foldable[List].getClass
    }
  }

  "Reducible from Foldable" should {
    import cats.instances.list._, cats.instances.int._, cats.instances.tuple._,
    cats.instances.string._
    checkLaws(
      cats.laws.discipline.ReducibleTests[NonEmptyF[List, *]].reducible[(String, *), Int, Int].all
    )
  }

  // merely checking that too much evidence doesn't result in ambiguous
  // lookup
  object UnambiguousResolutionTests {
    def foldableReducible[F[_]: Reducible] = Foldable[NonEmptyF[F, *]]
    def foldableTraverse[F[_]: Traverse] = Foldable[NonEmptyF[F, *]]
    @annotation.nowarn("cat=unused&msg=evidence")
    def foldableAll[F[_]: Reducible: Traverse] = Foldable[NonEmptyF[F, *]]
  }
}

object CatsInstancesSpec {
  implicit def `nonempty list arb`[A](implicit A: Arbitrary[A]): Arbitrary[NonEmptyF[List, A]] =
    Arbitrary(arbitrary[(A, List[A])].map { case (hd, tl) => (hd +-: tl).toNEF })

  implicit def `nonempty f eq`[F[_], A](implicit FA: Eq[F[A]]): Eq[NonEmptyF[F, A]] = {
    type T[k[_]] = Eq[k[A]]
    NonEmptyColl.Instance.substF[T, F](FA)
  }
}
