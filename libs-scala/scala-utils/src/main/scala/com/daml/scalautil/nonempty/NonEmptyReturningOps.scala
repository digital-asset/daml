// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import scala.collection.{immutable => imm}, imm.Map
import NonEmptyCollCompat._

/** Functions where ''the receiver'' is non-empty can be found implicitly with
  * no further imports; they "just work".  However, if we wish to refine a
  * method on a built-in type that merely ''returns'' a nonempty-annotated type, we
  * must import the contents of this object.
  */
object NonEmptyReturningOps {
  implicit final class `NE Iterable Ops`[A, CC[_], C](
      private val self: IterableOps[A, CC, C with imm.Iterable[A]]
  ) {
    def groupBy1[K](f: A => K): Map[K, NonEmpty[C]] =
      NonEmpty.subst[Lambda[f[_] => Map[K, f[C]]]](self groupBy f)

    // ideas for extension: +-: and :-+ operators
  }
}
