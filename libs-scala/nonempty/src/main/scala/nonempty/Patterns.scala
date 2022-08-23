// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import scala.collection.{immutable => imm}
import NonEmptyCollCompat._

/** Total version of [[+:]]. */
object +-: {
  def unapply[A, CC[_], C](t: NonEmpty[SeqOps[A, CC, C]]): Some[(A, C)] =
    Some((t.head, t.tail))
}

/** Total version of [[:+]]. */
object :-+ {
  def unapply[A, CC[_], C](t: NonEmpty[SeqOps[A, CC, C]]): Some[(C, A)] =
    Some((t.init, t.last))
}

/** The sole element of an iterable.  Works with sets and maps.
  *
  * These patterns form a complete partition of `NonEmpty[T]` where `T` is
  * a Seq.  Note that empty is excluded by the type.
  *
  * {{{
  *  case Singleton(hd) =>
  *  case hd +-: NonEmpty(tl) =>
  * }}}
  */
object Singleton {
  def unapply[A](t: NonEmpty[imm.Iterable[A]]): Option[A] =
    if (t.sizeIs == 1) Some(t.head1) else None
}
