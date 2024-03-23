// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalacheck.Gen.listOfN
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatest.Assertions.succeed
import org.scalatest.exceptions.TestFailedException
import scalaz.{Applicative, Monoid}
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}

trait AsyncForAll {
  final def forAllAsync[A](trials: Int)(
      f: A => Future[Assertion]
  )(implicit A: Arbitrary[A], ec: ExecutionContext): Future[Assertion] = {
    val runs = listOfN(trials, A.arbitrary).sample
      .getOrElse(sys error "random Gen failed")

    implicit val assertionMonoid: Monoid[Future[Assertion]] =
      Monoid.liftMonoid(Applicative[Future], Monoid.instance((_, result) => result, succeed))
    runs foldMap { a =>
      f(a) recoverWith { case ae: TestFailedException =>
        Future failed ae.modifyMessage(_ map { msg: String =>
          msg +
            "\n  Random parameters:" +
            s"\n    arg0: $a"
        })
      }
    }
  }
}
