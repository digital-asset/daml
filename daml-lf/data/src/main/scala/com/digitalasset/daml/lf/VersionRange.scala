// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scala.Ordering.Implicits.infixOrderingOps

/**
  * [[VersionRange]] represents a range of versions of
  * [[com.daml.lf.language.LanguageVersion]],
  * [[com.daml.lf.transaction.TransactionVersion]], or
  * [[com.daml.lf.value.ValueVersion]].
  *
  * @param min the minimal version included in the range.
  * @param max the maximal version included in the range.
  * @tparam V either [[com.daml.lf.language.LanguageVersion]],
  *   [[com.daml.lf.transaction.TransactionVersion]], or
  *   [[com.daml.lf.value.ValueVersion]].
  */
final case class VersionRange[V](
    min: V,
    max: V,
)(implicit ordering: Ordering[V]) {

  def intersect(that: VersionRange[V]): VersionRange[V] =
    VersionRange(
      min = this.min max that.min,
      max = this.max min that.max,
    )

  def contains(v: V): Boolean =
    min <= v && v <= max

}
