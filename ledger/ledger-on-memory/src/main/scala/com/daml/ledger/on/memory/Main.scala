// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.Runner
import com.digitalasset.resources.ProgramResource

import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def main(args: Array[String]): Unit = {
    new ProgramResource(
      Runner("In-Memory Ledger", InMemoryLedgerReaderWriter.owner(_, _)).owner(args)).run()
  }
}
