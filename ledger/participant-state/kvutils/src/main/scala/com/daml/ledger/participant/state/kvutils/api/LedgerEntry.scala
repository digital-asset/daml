// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.Offset

/**
  * Defines a log entry or update that may be read from the ledger.
  */
sealed trait LedgerEntry

object LedgerEntry {

  /**
    * A log entry read from the ledger.
    * @param offset  offset of log entry
    * @param entryId opaque ID of log entry
    * @param envelope  opaque contents of log entry
    */
  final class LedgerRecord(
      val offset: Offset,
      val entryId: Bytes,
      val envelope: Bytes
  ) extends LedgerEntry

  /**
    * A heart beat read from the ledger.
    * @param offset  offset of heartbeat log entry
    * @param instant timestamp of heartbeat
    */
  final case class Heartbeat(offset: Offset, instant: Instant) extends LedgerEntry

  object LedgerRecord {
    def apply(offset: Offset, entryId: Bytes, envelope: Bytes): LedgerRecord =
      new LedgerRecord(offset, entryId, envelope)

    def unapply(record: LedgerRecord): Option[(Offset, Bytes, Bytes)] =
      Some((record.offset, record.entryId, record.envelope))
  }

}
