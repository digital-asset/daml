// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

case class Range(startExclusive: Offset, endInclusive: Offset) {
  import Range._
  assert(
    startExclusive <= endInclusive,
    s"Start offset $startExclusive cannot be lesser than end offset $endInclusive",
  )

  /** Relative complement of given rangeToDiff in this one (this - rangeToDiff) that is lesser than end of given rangeToDiff (this.end < rangeToDiff.start)
    * @return range representing set of offsets that are in this range but not in rangeToDiff where each offset is lesser than rangeToDiff.start
    */
  def lesserRangeDifference(rangeToDiff: Range): Option[Range] = {
    if (isSubsetOf(rangeToDiff)) {
      None
    } else {
      if (startExclusive >= rangeToDiff.startExclusive) {
        None
      } else {
        Some(Range(startExclusive, Range.min(endInclusive, rangeToDiff.startExclusive)))
      }
    }
  }

  /** Relative complement of given rangeToDiff in this one (this - rangeToDiff) that is greater than start of given rangeToDiff (this.start > rangeToDiff.end)
    * @return range representing set of offsets that are in this range but not in rangeToDiff where each offset is greater than rangeToDiff.end
    */
  def greaterRangeDifference(rangeToDiff: Range): Option[Range] = {
    if (isSubsetOf(rangeToDiff)) {
      None
    } else {
      if (endInclusive <= rangeToDiff.endInclusive) {
        None
      } else {
        Some(Range(Range.max(startExclusive, rangeToDiff.endInclusive), endInclusive))
      }
    }
  }

  /** checks if this range is subset of given one
    */
  def isSubsetOf(range: Range): Boolean =
    startExclusive >= range.startExclusive && endInclusive <= range.endInclusive

  /** checks if this and given range are disjointed - they have no offset in common.
    */
  def areDisjointed(range: Range): Boolean =
    startExclusive >= range.endInclusive || endInclusive <= range.startExclusive

  /** returns intersection of this and given range - range representing set of offsets that are in this and given range.
    */
  def intersect(range: Range): Option[Range] = {
    if (areDisjointed(range)) {
      None
    } else {
      Some(
        Range(
          max(startExclusive, range.startExclusive),
          min(endInclusive, range.endInclusive),
        )
      )
    }
  }

  /** checks if ranges are consecutive
    */
  def areConsecutive(range: Range): Boolean =
    startExclusive == range.endInclusive || endInclusive == range.startExclusive
}

object Range {

  def max(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  def min(offset1: Offset, offset2: Offset): Offset =
    if (offset1 < offset2) offset1 else offset2
}
