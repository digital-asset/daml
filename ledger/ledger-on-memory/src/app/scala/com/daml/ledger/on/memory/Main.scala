// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter.Index
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.SimpleLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig, Runner}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.resources.{ProgramResource, ResourceOwner}

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit = {
    val resource = for {
      dispatcher <- InMemoryLedgerReaderWriter.dispatcher
      factory = new InMemoryLedgerFactory(dispatcher)
      runner <- new Runner("In-Memory Ledger", factory).owner(args)
    } yield runner

    new ProgramResource(resource).run()
  }

  class InMemoryLedgerFactory(dispatcher: Dispatcher[Index])
      extends SimpleLedgerFactory[InMemoryLedgerReaderWriter] {

    // Here we share the ledger state access object, which also encapsulates InMemoryState,
    // so that the state can be shared amongst multiple participants for this in-memory ledger.
    val ledgerStateAccess = new InMemoryLedgerStateAccess()(ExecutionContext.global)

    override def owner(config: Config[Unit], participantConfig: ParticipantConfig)(
        implicit executionContext: ExecutionContext,
        materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[InMemoryLedgerReaderWriter] =
      InMemoryLedgerReaderWriter.owner(
        config.ledgerId,
        participantConfig.participantId,
        dispatcher = dispatcher,
        ledgerStateAccess = ledgerStateAccess
      )
  }
}
