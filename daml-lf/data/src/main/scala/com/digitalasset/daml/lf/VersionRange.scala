// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
final case class VersionRange[V] private (
    min: V,
    max: V,
)(implicit ordering: Ordering[V])
    extends data.NoCopy {

  def join(that: VersionRange[V]): VersionRange[V] =
    new VersionRange(
      min = this.min min that.min,
      max = this.max max that.max,
    )

  def join(version: V): VersionRange[V] = join(VersionRange(version))

  private[lf] def map[W](f: V => W)(implicit ordering: Ordering[W]): VersionRange[W] =
    VersionRange(f(min), f(max))

  def contains(v: V): Boolean =
    min <= v && v <= max
}

object VersionRange {

  def apply[V](min: V, max: V)(implicit ordering: Ordering[V]): VersionRange[V] = {
    assert(min <= max)
    new VersionRange(min, max)
  }

  def apply[V](version: V)(implicit ordering: Ordering[V]): VersionRange[V] =
    new VersionRange(version, version)

  // We represent an empty Range by setting min and max to the max/min possible value
  // O(n)
  private[lf] def slowEmpty[V](allValues: Seq[V])(implicit ordering: Ordering[V]): VersionRange[V] =
    new VersionRange(allValues.max, allValues.min)

}
