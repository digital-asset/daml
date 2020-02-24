// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Configuration, LedgerId, Offset, TimeModel}
import com.digitalasset.ledger.api.health.ReportsHealth

trait LedgerReader extends ReportsHealth {
  def events(offset: Option[Offset]): Source[LedgerEntry, NotUsed]

  /**
    * Get the ledger's ID from which this reader instance streams events.
    * Should not be a blocking operation.
    * @return  ID of the ledger from which this reader streams events
    */
  def ledgerId(): LedgerId
}

object LedgerReader {
  val DefaultConfiguration = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )
}
