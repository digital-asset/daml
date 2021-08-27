// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.offset.Offset

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

/** Helper functions for generating versioned 16 byte [[Offset]]s from integers.
  * The created offset will look as follows:
  * | version (8 bits) | highest index (56 bits) | middle index (32 bits) | lowest index (32 bits) |
  * Leading zeros will be retained when generating the resulting offset bytes.
  *
  * Example usage:
  *
  *   - If you have one record per block then use [[of]] with the block ID as the second and last argument.
  *   - If you may have multiple records per block then use [[of]] with the index within the block as the third argument.
  *
  * @see com.daml.ledger.offset.Offset
  */
class VersionedOffsetBuilder(version: Byte) {
  import VersionedOffsetBuilder._

  def onlyKeepHighestIndex(offset: Offset): Offset = {
    val highest = highestIndex(offset)
    of(highest)
  }

  def dropLowestIndex(offset: Offset): Offset = {
    val (highest, middle, _) = split(offset)
    of(highest, middle)
  }

  def setMiddleIndex(offset: Offset, middle: Int): Offset = {
    val (highest, _, lowest) = split(offset)
    of(highest, middle, lowest)
  }

  def setLowestIndex(offset: Offset, lowest: Int): Offset = {
    val (highest, middle, _) = split(offset)
    of(highest, middle, lowest)
  }

  def of(first: Long, second: Int = 0, third: Int = 0): Offset = {
    if (first < 0 || first > MaxHighest)
      throw new IllegalArgumentException(s"Highest: $first is out of range [0, $MaxHighest]")

    val bytes = new ByteArrayOutputStream
    val stream = new DataOutputStream(bytes)
    stream.writeByte(version.toInt)
    writeHighest(first, stream)
    stream.writeInt(second)
    stream.writeInt(third)
    Offset.fromByteArray(bytes.toByteArray)
  }
}

object VersionedOffsetBuilder {

  val MaxHighest: Long = (1L << 56) - 1

  private val highestStartByte = 1
  private val highestSizeBytes = 7
  private val highestMask = 0x00ffffffffffffffL

  def apply(version: Byte): VersionedOffsetBuilder = new VersionedOffsetBuilder(version)

  def version(offset: Offset): Byte = {
    val stream = toDataInputStream(offset)
    stream.readByte()
  }

  // `highestIndex` is used a lot, so it's worth optimizing a little rather than reusing `split`.
  def highestIndex(offset: Offset): Long = {
    val stream = toDataInputStream(offset)
    readHighest(stream)
  }

  def middleIndex(offset: Offset): Int = split(offset)._2

  def lowestIndex(offset: Offset): Int = split(offset)._3

  private[kvutils] def split(offset: Offset): (Long, Int, Int) = {
    val stream = toDataInputStream(offset)
    val highest = readHighest(stream)
    val middle = stream.readInt()
    val lowest = stream.readInt()
    (highest, middle, lowest)
  }

  private def toDataInputStream(offset: Offset) = new DataInputStream(
    new ByteArrayInputStream(offset.toByteArray)
  )

  private def readHighest(stream: DataInputStream) = {
    val versionAndHighest = stream.readLong()
    versionAndHighest & highestMask
  }

  private def writeHighest(highest: Long, stream: DataOutputStream): Unit = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(highest)
    stream.write(buffer.array(), highestStartByte, highestSizeBytes)
  }
}
