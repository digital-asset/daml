// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.Offset

sealed trait LedgerEntry {
  def offset: Offset
}

object LedgerEntry {

  final class LedgerRecord(
      val offset: Offset,
      val entryId: DamlLogEntryId,
      val envelope: Array[Byte]
  ) extends LedgerEntry

  final case class Heartbeat(offset: Offset, instant: Instant) extends LedgerEntry

  object LedgerRecord {
    def apply(offset: Offset, entryId: DamlLogEntryId, envelope: Array[Byte]): LedgerRecord =
      new LedgerRecord(offset, entryId, envelope)

    def unapply(record: LedgerRecord): Option[(Offset, DamlLogEntryId, Array[Byte])] =
      Some((record.offset, record.entryId, record.envelope))
  }

}
