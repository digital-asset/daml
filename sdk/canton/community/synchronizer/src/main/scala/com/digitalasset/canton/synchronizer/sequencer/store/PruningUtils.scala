// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.PositiveFiniteDuration

import java.time.Duration

object PruningUtils {

  /** Generates time intervals for pruning based on a minimum timestamp, an upper limit. For
    * non-empty output the first (lower) interval always starts at CantonTimestamp.MinValue, and the
    * last (upper) interval always ends at the given `upTo` timestamp.
    *
    * @param minO
    *   The minimum timestamp of a record in the database
    * @param upTo
    *   Upper bound for the intervals
    * @param step
    *   Max duration of the interval
    * @return
    *   A sequence of timestamp pairs representing intervals
    */
  def pruningTimeIntervals(
      minO: Option[CantonTimestamp],
      upTo: CantonTimestamp,
      step: PositiveFiniteDuration,
  ): Seq[(CantonTimestamp, CantonTimestamp)] = {
    val stepDuration: Duration = step.duration
    minO match {
      case Some(min) if min <= upTo =>
        (
          Seq(CantonTimestamp.MinValue).iterator
            ++ Iterator
              .iterate(min)(_.add(stepDuration))
              .takeWhile(_ < upTo) ++
            Seq(upTo)
        )
          .sliding(2)
          .collect { case Seq(from, to) =>
            from -> to
          }
          .toSeq
      case _ =>
        Seq.empty[(CantonTimestamp, CantonTimestamp)]
    }
  }
}
