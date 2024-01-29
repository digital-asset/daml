// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalatest.{Assertion, Succeeded}
import scalaz.syntax.show._
import scalaz.{Equal, Show}

object CustomMatcher {

  final implicit class CustomMatcherOps[A](val underlying: A) extends AnyVal {

    def should_===(other: A)(implicit eqEv: Equal[A], showEv: Show[A]): Assertion =
      if (eqEv.equal(underlying, other)) Succeeded
      else reportFailure(underlying, " =/= ", other)

    def should_=/=(other: A)(implicit eqEv: Equal[A], showEv: Show[A]): Assertion =
      if (eqEv.equal(underlying, other)) reportFailure(underlying, " === ", other)
      else Succeeded

    private def reportFailure(underlying: A, str: String, other: A)(implicit
        showEv: Show[A]
    ): Assertion =
      throw CustomMatcherException(s"${underlying.shows}$str${other.shows}")
  }

  case class CustomMatcherException(c: String) extends RuntimeException(c)
}
