// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.Runner

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  Runner(
    "In-Memory Ledger",
    participantId => new InMemoryLedgerReaderWriter(participantId = participantId),
  ).run(args)
}
