// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scala.Ordering.Implicits.infixOrderingOps

/** [[VersionRange]] represents a range of versions of
  * [[com.daml.lf.language.LanguageVersion]] or
  * [[com.daml.lf.transaction.TransactionVersion]].
  *
  * @param min the minimal version included in the range.
  * @param max the maximal version included in the range.
  * @tparam V either [[com.daml.lf.language.LanguageVersion]] or
  *   [[com.daml.lf.transaction.TransactionVersion]].
  */
final case class VersionRange[V](
    min: V,
    max: V,
)(implicit ordering: Ordering[V]) {

  require(min <= max)

  private[lf] def map[W](f: V => W)(implicit ordering: Ordering[W]) =
    VersionRange(f(min), f(max))

  def contains(v: V): Boolean =
    min <= v && v <= max

  def intersects(other: VersionRange[V]): Boolean =
    other.min <= max && min <= other.max
}
