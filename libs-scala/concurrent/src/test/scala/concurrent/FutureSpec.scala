// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.concurrent

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.test.illTyped

import scala.annotation.nowarn
import scala.{concurrent => sc}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
@nowarn("msg=local method example .* is never used")
class FutureSpec extends AnyWordSpec with Matchers {
  import ExecutionContextSpec._

  val elephantVal = 3000
  val catVal = 9
  val untypedVal = -1

  val someElephantFuture: Future[Elephant, Int] = sc.Future successful elephantVal
  val someCatFuture: Future[Cat, Int] = sc.Future successful catVal
  val someUntypedFuture: sc.Future[Int] = sc.Future successful untypedVal

  // we repeat imports below to show exactly what imports are needed for a given
  // scenario.  Naturally, in real code, you would not be so repetitive.

  "an untyped future" can {
    "be flatmapped to by any future" in {
      import scalaz.syntax.bind._, TestImplicits.Elephant
      def example = someElephantFuture flatMap (_ => someUntypedFuture)
    }

    "simply become a typed future" in {
      def example: Future[Cat, Int] = someUntypedFuture
    }
  }

  "a well-typed future" should {
    "not lose its type to conversion" in {
      illTyped(
        "someCatFuture: sc.Future[Int]",
        "type mismatch.*found.*daml.concurrent.Future.*required: scala.concurrent.Future.*",
      )
    }
  }

  "two unrelated futures" should {
    "mix their types if zipped" in {
      // putting in Set (an invariant context) lets us check the inferred type
      def example = Set(someElephantFuture zip someCatFuture)
      example: Set[Future[Elephant with Cat, (Int, Int)]]
    }

    "disallow mixing in flatMap" in {
      import scalaz.syntax.bind._, TestImplicits.Elephant
      illTyped(
        "someElephantFuture flatMap (_ => someCatFuture)",
        "type mismatch.*found.*Cat.*required.*Elephant.*",
      )
    }

    "allow mixing in flatMap if requirement changed first" in {
      import scalaz.syntax.bind._, TestImplicits.Cat
      def example =
        someElephantFuture
          .changeExecutionContext[Cat]
          .flatMap(_ => someCatFuture)
    }

    "continue a chain after requirements tightened" in {
      import scalaz.syntax.bind._, TestImplicits.cryptozoology
      def example =
        someElephantFuture
          .require[Elephant with Cat]
          .flatMap(_ => someCatFuture)
    }

    "disallow require on unrelated types" in {
      illTyped("someElephantFuture.require[Cat]", "type arguments.*do not conform.*")
    }
  }
}
