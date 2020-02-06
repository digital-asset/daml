// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.SimpleLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, ResourceOwner}

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("In-Memory Ledger", InMemoryLedgerFactory).owner(args)).run()
  }

  object InMemoryLedgerFactory extends SimpleLedgerFactory[InMemoryLedgerReaderWriter] {
    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: Unit
    )(
        implicit executionContext: ExecutionContext,
        materializer: Materializer,
    ): ResourceOwner[InMemoryLedgerReaderWriter] =
      InMemoryLedgerReaderWriter.owner(ledgerId, participantId)
  }
}
