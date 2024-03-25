// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck.Properties
import org.scalactic.{Prettifier, source}

/** Integration of Scalatest [[AnyWordSpec]] with Scalaz law checkers, or any other
  * purely Scalacheck-defined tests, for that matter.  Each invocation should go
  * in a separate `should` category, as test names will collide otherwise.
  *
  * Usage:
  *
  * {{{
  *  "Blah Functor" should {
  *    checkLaws(ScalazProperties.functor.laws[Blah])
  *  }
  * }}}
  */
trait WordSpecCheckLaws extends Checkers { this: AnyWordSpec =>

  /** Like `check(props)` but with '''much better''' test reporting. */
  def checkLaws(props: Properties)(implicit
      generatorDrivenConfig: PropertyCheckConfiguration,
      prettifier: Prettifier,
      pos: source.Position,
  ): Unit =
    props.properties foreach { case (s, p) => s in check(p) }
}
