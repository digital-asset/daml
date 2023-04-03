// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck.Properties
import org.scalactic.{Prettifier, source}

/** Integration of Scalatest [[AnyFreeSpec]] with Scalaz law checkers, or any other
  * purely Scalacheck-defined tests, for that matter.  Each invocation should go
  * in a separate `should` category, as test names will collide otherwise.
  *
  * Usage:
  *
  * {{{
  *  "Blah Functor" - {
  *    checkLaws(ScalazProperties.functor.laws[Blah])
  *  }
  * }}}
  */
trait FreeSpecCheckLaws extends Checkers { this: AnyFreeSpec =>

  /** Like `check(props)` but with '''much better''' test reporting. */
  def checkLaws(props: Properties)(implicit
      generatorDrivenConfig: PropertyCheckConfiguration,
      prettifier: Prettifier,
      pos: source.Position,
  ): Unit = {
    // FreeSpec can't deal with duplicate test names; a duplicate name
    // means test reached by diamond inheritance twice anyway so just skip it
    props.properties.distinctBy(_._1) foreach { case (s, p) => s in check(p) }
  }
}
