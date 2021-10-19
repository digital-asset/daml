// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.KVOffset._

private[kvutils] final case class KVOffset(offset: Offset) {
  lazy val (version, highest, middle, lowest) = {
    val stream = new DataInputStream(offset.toInputStream)
    val versionAndHighest = stream.readLong()
    val version = ((versionAndHighest & VersionMask) >> 56).toByte
    val highest = versionAndHighest & HighestMask
    val middle = stream.readInt()
    val lowest = stream.readInt()
    (version, highest, middle, lowest)
  }

  def zeroLowest: KVOffset =
    setLowest(0)

  def setLowest(newLowest: Int): KVOffset =
    KVOffset(new KVOffsetBuilder(version).of(highest, middle, newLowest))
}

object KVOffset {
  val MaxHighest: Long = (1L << 56) - 1
  val VersionMask: Long = 0xff00000000000000L
  val HighestMask: Long = 0x00ffffffffffffffL
  private val HighestStartByte = 1
  private val HighestSizeBytes = 7

  def of(version: Byte, highest: Long, middle: Int, lowest: Int): KVOffset = {
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
    KVOffset(Offset.fromByteArray(bytes.toByteArray))
  }

  private def writeHighest(highest: Long, stream: DataOutputStream): Unit = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(highest)
    stream.write(buffer.array(), HighestStartByte, HighestSizeBytes)
  }
}
