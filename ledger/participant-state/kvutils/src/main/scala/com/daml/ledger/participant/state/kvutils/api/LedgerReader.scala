// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, LedgerId, Offset, TimeModel}
import com.digitalasset.ledger.api.health.ReportsHealth

class LedgerRecord(val offset: Offset, val entryId: DamlLogEntryId, val envelope: Array[Byte]) {}

object LedgerRecord {
  def apply(offset: Offset, entryId: DamlLogEntryId, envelope: Array[Byte]): LedgerRecord =
    new LedgerRecord(offset, entryId, envelope)
}

trait LedgerReader extends ReportsHealth with AutoCloseable {
  def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed]

  def retrieveLedgerId(): LedgerId
}

object LedgerReader {
  val DefaultTimeModel = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )
}
