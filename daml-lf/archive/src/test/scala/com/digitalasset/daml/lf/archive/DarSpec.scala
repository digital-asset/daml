// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class DarSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  behavior of Dar.getClass.getSimpleName

  it should "implement Functor with proper map" in forAll(darGen[Int]) { dar =>
    import scalaz.Functor
    import scalaz.syntax.functor._

    implicit val sut: Functor[Dar] = Dar.darFunctor

    def f(a: Int): Int = a + 100

    dar.map(f) shouldBe Dar(f(dar.main), dar.dependencies.map(f))
  }

  private def darGen[A: Arbitrary]: Gen[Dar[A]] =
    for {
      main <- Arbitrary.arbitrary[A]
      dependencies <- Arbitrary.arbitrary[List[A]]
    } yield Dar[A](main, dependencies)
}
