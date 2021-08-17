// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.daml.ledger.offset.Offset

/** Helper functions for generating 16 byte [[Offset]]s from integers.
  * The created offset will look as follows:
  * | highest index (64 bits) | middle index (32 bits) | lowest index (32 bits) |
  * Leading zeros will be retained when generating the resulting offset bytes.
  *
  * Example usage:
  *
  *   - If you have one record per block then use [[OffsetBuilder.fromLong]] with a single argument, the block ID.
  *   - If you may have multiple records per block then use [[OffsetBuilder.fromLong]] with the index within the block as the second argument.
  *
  * @see com.daml.ledger.offset.Offset
  * @see com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReader
  */
object OffsetBuilder {
  private[kvutils] val highestStart = 0
  private[kvutils] val middleStart = 8
  private[kvutils] val lowestStart = 12
  private[kvutils] val end = 16

  def onlyKeepHighestIndex(offset: Offset): Offset = {
    val highest = highestIndex(offset)
    fromLong(highest)
  }

  def dropLowestIndex(offset: Offset): Offset = {
    val (highest, middle, _) = split(offset)
    fromLong(highest, middle)
  }

  def setMiddleIndex(offset: Offset, middle: Int): Offset = {
    val (highest, _, lowest) = split(offset)
    fromLong(highest, middle, lowest)
  }

  def setLowestIndex(offset: Offset, lowest: Int): Offset = {
    val (highest, middle, _) = split(offset)
    fromLong(highest, middle, lowest)
  }

  def fromLong(first: Long, second: Int = 0, third: Int = 0): Offset = {
    val bytes = new ByteArrayOutputStream
    val stream = new DataOutputStream(bytes)
    stream.writeLong(first)
    stream.writeInt(second)
    stream.writeInt(third)
    Offset.fromByteArray(bytes.toByteArray)
  }

  // `highestIndex` is used a lot, so it's worth optimizing a little rather than reusing `split`.
  def highestIndex(offset: Offset): Long = {
    val stream = new DataInputStream(new ByteArrayInputStream(offset.toByteArray))
    stream.readLong()
  }

  def middleIndex(offset: Offset): Int = split(offset)._2

  def lowestIndex(offset: Offset): Int = split(offset)._3

  def split(offset: Offset): (Long, Int, Int) = {
    val stream = new DataInputStream(new ByteArrayInputStream(offset.toByteArray))
    val highest = stream.readLong()
    val middle = stream.readInt()
    val lowest = stream.readInt()
    (highest, middle, lowest)
  }
}
