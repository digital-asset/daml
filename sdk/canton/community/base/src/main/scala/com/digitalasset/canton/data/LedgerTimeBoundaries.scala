// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.daml.lf.data.Time

/** Time range wrapper that also provides accessors that return None where the values are
  * unconstrained
  */
final case class LedgerTimeBoundaries(range: Time.Range) {
  def minConstraint: Option[Time.Timestamp] =
    Option.when(range.min != Time.Range.unconstrained.min)(range.min)
  def maxConstraint: Option[Time.Timestamp] =
    Option.when(range.max != Time.Range.unconstrained.max)(range.max)
}

object LedgerTimeBoundaries {
  val unconstrained: LedgerTimeBoundaries = LedgerTimeBoundaries(Time.Range.unconstrained)
  def fromConstraints(
      min: Option[Time.Timestamp],
      max: Option[Time.Timestamp],
  ): LedgerTimeBoundaries =
    LedgerTimeBoundaries(
      Time.Range(
        min.getOrElse(Time.Range.unconstrained.min),
        max.getOrElse(Time.Range.unconstrained.max),
      )
    )
}
