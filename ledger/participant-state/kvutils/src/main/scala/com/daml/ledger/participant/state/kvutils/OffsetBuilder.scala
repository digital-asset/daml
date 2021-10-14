// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.offset.Offset

/** Helper functions for generating 16 byte [[Offset]]s from integers.
  * The created offset will look as follows:
  * | zeros (8 bits) | highest index (56 bits) | middle index (32 bits) | lowest index (32 bits) |
  * Leading zeros will be retained when generating the resulting offset bytes.
  *
  * Example usage:
  *
  *   - If you have one record per block then use [[OffsetBuilder.fromLong]] with a single argument, the block ID.
  *   - If you may have multiple records per block then use [[OffsetBuilder.fromLong]] with the index within the block as the second argument.
  *
  * @see com.daml.ledger.offset.Offset
  * @see com.daml.ledger.offset.VersionedOffsetBuilder
  * @see com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReader
  */
object OffsetBuilder {
  private[kvutils] val highestStart = 0
  private[kvutils] val middleStart = 8
  private[kvutils] val lowestStart = 12
  private[kvutils] val end = 16

  private val delegate = new VersionedOffsetBuilder(version = 0)

  def onlyKeepHighestIndex(offset: Offset): Offset = delegate.onlyKeepHighestIndex(offset)

  def dropLowestIndex(offset: Offset): Offset = delegate.dropLowestIndex(offset)

  def setMiddleIndex(offset: Offset, middle: Int): Offset = delegate.setMiddleIndex(offset, middle)

  def setLowestIndex(offset: Offset, lowest: Int): Offset = delegate.setLowestIndex(offset, lowest)

  def fromLong(first: Long, second: Int = 0, third: Int = 0): Offset =
    delegate.of(first, second, third)

  def highestIndex(offset: Offset): Long = delegate.highestIndex(offset)

  def middleIndex(offset: Offset): Int = delegate.middleIndex(offset)

  def lowestIndex(offset: Offset): Int = delegate.lowestIndex(offset)

  private[kvutils] def split(offset: Offset): (Long, Int, Int) = {
    val (highest, middle, lowest) = delegate.split(offset)
    (highest, middle, lowest)
  }
}
