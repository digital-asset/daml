// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.transaction.VersionTimeline

final case class VersionRange[V](
    min: V,
    max: V,
) {
  import VersionTimeline._
  import VersionTimeline.Implicits._

  def intersect(that: VersionRange[V])(
      implicit ev: VersionTimeline.SubVersion[V]): VersionRange[V] =
    VersionRange(
      min = maxVersion(this.min, that.min),
      max = minVersion(this.max, that.max)
    )

  def nonEmpty(implicit ev: VersionTimeline.SubVersion[V]): Boolean =
    !(max precedes min)

  def contains(v: V)(implicit ev: VersionTimeline.SubVersion[V]): Boolean =
    !((max precedes v) || (v precedes min))

  def map[W](f: V => W): VersionRange[W] =
    VersionRange(f(min), f(max))

}
