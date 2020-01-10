// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Configuration, LedgerId, Offset, TimeModel}
import com.digitalasset.ledger.api.health.ReportsHealth

trait LedgerReader extends ReportsHealth with AutoCloseable {
  def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed]

  def retrieveLedgerId(): LedgerId
}

object LedgerReader {
  val DefaultConfiguration = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )
}
