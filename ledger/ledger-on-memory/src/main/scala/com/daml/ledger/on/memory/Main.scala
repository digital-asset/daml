// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.Runner

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  Runner(
    "In-Memory Ledger",
    (ledgerId, participantId) => new InMemoryLedgerReaderWriter(ledgerId, participantId),
  ).run(args)
}
