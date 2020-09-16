// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.transaction.VersionTimeline

/**
  * [[VersionRange]] represents a range of versions of
  * [[com.daml.lf.language.LanguageVersion]],
  * [[com.daml.lf.transaction.TransactionVersion]], or
  * [[com.daml.lf.value.ValueVersion]].
  *
  * The order of versions is specified by [[VersionTimeline]].
  *
  * @param min the minimal version included in the range.
  * @param max the maximla version included in the range.
  * @tparam V either [[com.daml.lf.language.LanguageVersion]],
  *   [[com.daml.lf.transaction.TransactionVersion]], or
  *   [[com.daml.lf.value.ValueVersion]].
  */
final case class VersionRange[V](
    min: V,
    max: V,
)(implicit ev: VersionTimeline.SubVersion[V]) {
  import VersionTimeline._
  import VersionTimeline.Implicits._

  def intersect(that: VersionRange[V])(
      implicit ev: VersionTimeline.SubVersion[V]): VersionRange[V] =
    VersionRange(
      min = maxVersion(this.min, that.min),
      max = minVersion(this.max, that.max)
    )

  def isEmpty: Boolean =
    max precedes min

  def nonEmpty: Boolean =
    !isEmpty

  def contains(v: V): Boolean =
    !((max precedes v) || (v precedes min))

}
