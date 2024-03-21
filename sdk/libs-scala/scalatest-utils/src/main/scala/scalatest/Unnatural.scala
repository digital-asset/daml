// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalatest

import org.scalacheck.Arbitrary
import scalaz.Equal

/** `A` whose equalIsNatural == false.  Useful when testing [[Equal]] instances
  * that short-circuit when the tparam's equalIsNatural, so you're testing the
  * handwritten paths rather than the compiler-generated ones.
  */
private[daml] final case class Unnatural[+A](a: A)

private[daml] object Unnatural {
  implicit def arbUA[A: Arbitrary]: Arbitrary[Unnatural[A]] =
    Arbitrary(Arbitrary.arbitrary[A] map (Unnatural(_)))
  implicit def eqUA[A: Equal]: Equal[Unnatural[A]] = Equal.equalBy(_.a)
}
