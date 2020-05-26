// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.lf.data.FlatSpecCheckLaws
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FlatSpec, Matchers}
import scalaz.std.anyVal._
import scalaz.scalacheck.ScalazProperties

class DarSpec extends FlatSpec with Matchers with FlatSpecCheckLaws {
  behavior of s"${Dar.getClass.getSimpleName} Equal"
  checkLaws(ScalazProperties.equal.laws[Dar[Int]])

  behavior of s"${Dar.getClass.getSimpleName} Traverse"
  checkLaws(ScalazProperties.traverse.laws[Dar])

  behavior of s"${Dar.getClass.getSimpleName} Functor"
  checkLaws(ScalazProperties.functor.laws[Dar])

  private def darGen[A: Arbitrary]: Gen[Dar[A]] =
    for {
      main <- Arbitrary.arbitrary[A]
      dependencies <- Arbitrary.arbitrary[List[A]]
    } yield Dar[A](main, dependencies)

  private implicit def darArb[A: Arbitrary]: Arbitrary[Dar[A]] = Arbitrary(darGen)
}
