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
    val VersionedOffset(_, highest, middle, _) = split(offset)
    of(highest, middle)
  }

  def setMiddleIndex(offset: Offset, middle: Int): Offset = {
    val VersionedOffset(_, highest, _, lowest) = split(offset)
    of(highest, middle, lowest)
  }

  def setLowestIndex(offset: Offset, lowest: Int): Offset = {
    val VersionedOffset(_, highest, middle, _) = split(offset)
    of(highest, middle, lowest)
  }

  def of(highest: Long, middle: Int = 0, lowest: Int = 0): Offset = {
    require(
      highest >= 0 && highest <= MaxHighest,
      s"highest ($highest) is out of range [0, $MaxHighest]",
    )
    require(middle >= 0, s"middle ($middle) is lower than 0")
    require(lowest >= 0, s"lowest ($lowest) is lower than 0")

    val bytes = new ByteArrayOutputStream
    val stream = new DataOutputStream(bytes)
    stream.writeByte(version.toInt)
    writeHighest(highest, stream)
    stream.writeInt(middle)
    stream.writeInt(lowest)
    Offset.fromByteArray(bytes.toByteArray)
  }

  def version(offset: Offset): Byte = {
    val stream = toDataInputStream(offset)
    val extractedVersion = stream.readByte()
    validateVersion(extractedVersion)
    extractedVersion
  }

  def matchesVersionOf(offset: Offset): Boolean = {
    val stream = toDataInputStream(offset)
    val extractedVersion = stream.readByte()
    extractedVersion == version
  }

  // `highestIndex` is used a lot, so it's worth optimizing a little rather than reusing `split`.
  def highestIndex(offset: Offset): Long = {
    val stream = toDataInputStream(offset)
    val (extractedVersion, highest) = readVersionAndHighest(stream)
    validateVersion(extractedVersion)
    highest
  }

  def middleIndex(offset: Offset): Int = split(offset).middle

  def lowestIndex(offset: Offset): Int = split(offset).lowest

  private[kvutils] def split(offset: Offset): VersionedOffset = {
    val stream = toDataInputStream(offset)
    val (extractedVersion, highest) = readVersionAndHighest(stream)
    validateVersion(extractedVersion)
    val middle = stream.readInt()
    val lowest = stream.readInt()
    VersionedOffset(extractedVersion, highest, middle, lowest)
  }

  private def validateVersion(extractedVersion: Byte): Unit =
    require(version == extractedVersion, s"wrong version $extractedVersion, should be $version")
}

object VersionedOffsetBuilder {
  val MaxHighest: Long = (1L << 56) - 1

  private val highestStartByte = 1
  private val highestSizeBytes = 7
  private val versionMask = 0xff00000000000000L
  private val highestMask = 0x00ffffffffffffffL

  private def toDataInputStream(offset: Offset) =
    new DataInputStream(new ByteArrayInputStream(offset.toByteArray))

  private def readVersionAndHighest(stream: DataInputStream): (Byte, Long) = {
    val versionAndHighest = stream.readLong()
    val version = ((versionAndHighest & versionMask) >> 56).toByte
    val highest = versionAndHighest & highestMask
    version -> highest
  }

  private def writeHighest(highest: Long, stream: DataOutputStream): Unit = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(highest)
    stream.write(buffer.array(), highestStartByte, highestSizeBytes)
  }

  private[kvutils] final case class VersionedOffset(
      version: Byte,
      highest: Long,
      middle: Int,
      lowest: Int,
  )
}
