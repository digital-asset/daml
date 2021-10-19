// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.DataInputStream

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
class KVOffsetBuilder(version: Byte) {

  import KVOffsetBuilder._

  private val versionBytes = Bytes.fromByteArray(Array(version))

  def of(highest: Long, middle: Int = 0, lowest: Int = 0): Offset =
    KVOffset.of(version, highest, middle, lowest).offset

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

  private[kvutils] def split(offset: Offset): KVOffset = {
    validateVersion(offset)
    KVOffset(offset)
  }

  private def validateVersion(offset: Offset): Unit =
    require(
      offset.bytes.startsWith(versionBytes), {
        val extractedVersion = toDataInputStream(offset).readByte()
        s"wrong version $extractedVersion, should be $version"
      },
    )
}

object KVOffsetBuilder {
  private def toDataInputStream(offset: Offset) =
    new DataInputStream(offset.toInputStream)

  private def readHighest(stream: DataInputStream): Long = {
    val versionAndHighest = stream.readLong()
    versionAndHighest & KVOffset.HighestMask
  }
}
