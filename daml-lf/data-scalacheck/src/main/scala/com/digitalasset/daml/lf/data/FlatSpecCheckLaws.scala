// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Properties
import org.scalactic.{Prettifier, source}

/** Integration of Scalatest [[FlatSpec]] with Scalaz law checkers, or any other
  * purely Scalacheck-defined tests, for that matter.  Each invocation should go
  * in a separate `behavior of` category, as test names will collide otherwise.
  *
  * Usage:
  *
  * {{{
  *  behavior of "Blah Functor"
  *
  *  checkLaws(ScalazProperties.functor.laws[Blah])
  * }}}
  */
trait FlatSpecCheckLaws extends Checkers { this: FlatSpec =>

  /** Like `check(props)` but with '''much better''' test reporting. */
  def checkLaws(props: Properties)(
      implicit generatorDrivenConfig: PropertyCheckConfiguration,
      prettifier: Prettifier,
      pos: source.Position): Unit =
    props.properties foreach { case (s, p) => it should s in check(p) }
}
