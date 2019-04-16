// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import scalaz.std.anyVal._
import scalaz.scalacheck.ScalazProperties

class DarSpec extends FlatSpec with Matchers with Checkers {
  behavior of s"${Dar.getClass.getSimpleName} Equal"
  checkLaws(ScalazProperties.equal.laws[Dar[Int]])

  behavior of s"${Dar.getClass.getSimpleName} Traverse"
  checkLaws(ScalazProperties.traverse.laws[Dar])

  behavior of s"${Dar.getClass.getSimpleName} Functor"
  checkLaws(ScalazProperties.functor.laws[Dar])

  private def checkLaws(props: Properties): Unit =
    props.properties foreach { case (s, p) => it should s in check(p) }

  private def darGen[A: Arbitrary]: Gen[Dar[A]] =
    for {
      main <- Arbitrary.arbitrary[A]
      dependencies <- Arbitrary.arbitrary[List[A]]
    } yield Dar[A](main, dependencies)

  private implicit def darArb[A: Arbitrary]: Arbitrary[Dar[A]] = Arbitrary(darGen)
}
