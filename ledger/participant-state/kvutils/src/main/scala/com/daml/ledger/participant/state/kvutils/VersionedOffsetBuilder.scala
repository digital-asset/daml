// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Bytes

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

  private val versionBytes = Bytes.fromByteArray(Array(version))

  def dropLowestIndex(offset: Offset): Offset = {
    val VersionedOffset(_, highest, middle, _) = split(offset)
    of(highest, middle)
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
    validateVersion(offset)
    val stream = toDataInputStream(offset)
    stream.readByte()
  }

  def matchesVersionOf(offset: Offset): Boolean = {
    val stream = toDataInputStream(offset)
    val extractedVersion = stream.readByte()
    extractedVersion == version
  }

  // `highestIndex` is used a lot, so it's worth optimizing a little rather than reusing `split`.
  def highestIndex(offset: Offset): Long = {
    validateVersion(offset)
    val stream = toDataInputStream(offset)
    readHighest(stream)
  }

  private[kvutils] def split(offset: Offset): VersionedOffset = {
    validateVersion(offset)
    val stream = toDataInputStream(offset)
    val highest = readHighest(stream)
    val middle = stream.readInt()
    val lowest = stream.readInt()
    VersionedOffset(version, highest, middle, lowest)
  }

  private def validateVersion(offset: Offset): Unit =
    require(
      offset.bytes.startsWith(versionBytes), {
        val extractedVersion = toDataInputStream(offset).readByte()
        s"wrong version $extractedVersion, should be $version"
      },
    )
}

object VersionedOffsetBuilder {
  val MaxHighest: Long = (1L << 56) - 1

  private val highestStartByte = 1
  private val highestSizeBytes = 7
  private val highestMask = 0x00ffffffffffffffL

  private def toDataInputStream(offset: Offset) =
    new DataInputStream(offset.toInputStream)

  private def readHighest(stream: DataInputStream): Long = {
    val versionAndHighest = stream.readLong()
    versionAndHighest & highestMask
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
