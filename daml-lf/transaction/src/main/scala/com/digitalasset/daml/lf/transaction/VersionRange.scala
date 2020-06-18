// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.transaction.VersionTimeline

final case class VersionRange[V](
    min: V,
    max: V,
) {
  import VersionRange._
  import Ordering.Implicits._

  def intersect(that: VersionRange[V])(
      implicit ev: VersionTimeline.SubVersion[V]): VersionRange[V] = {
    VersionRange(
      min = this.min max that.min,
      max = this.max min that.max
    )
  }

  def nonEmpty(implicit ev: VersionTimeline.SubVersion[V]): Boolean =
    min <= max

  def contains(v: V)(implicit ev: VersionTimeline.SubVersion[V]): Boolean =
    min <= v && v <= max

}

object VersionRange {

  import VersionTimeline.Implicits._

  private implicit def ordering[V](implicit ev: VersionTimeline.SubVersion[V]): Ordering[V] =
    Ordering.fromLessThan(_ precedes _)

}
