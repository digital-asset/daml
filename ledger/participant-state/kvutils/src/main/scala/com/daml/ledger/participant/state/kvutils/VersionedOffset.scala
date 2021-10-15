// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.DataInputStream

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.VersionedOffset._

private[kvutils] final case class VersionedOffset(offset: Offset) {
  lazy val (version, highest, middle, lowest) = {
    val stream = new DataInputStream(offset.toInputStream)
    val versionAndHighest = stream.readLong()
    val version = ((versionAndHighest & VersionMask) >> 56).toByte
    val highest = versionAndHighest & HighestMask
    val middle = stream.readInt()
    val lowest = stream.readInt()
    (version, highest, middle, lowest)
  }
}

object VersionedOffset {
  private val VersionMask = 0xff00000000000000L
  private val HighestMask = 0x00ffffffffffffffL
}
