// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter.Index
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig, Runner}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.resources.{ProgramResource, ResourceOwner}

object Main {
  def main(args: Array[String]): Unit = {
    val resource = for {
      dispatcher <- InMemoryLedgerBatchingReaderWriter.dispatcher
      sharedState = new InMemoryState()
      factory = new InMemoryLedgerFactory(dispatcher, sharedState)
      runner <- new Runner("In-Memory Ledger", factory).owner(args)
    } yield runner

    new ProgramResource(resource).run()
  }

  class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
      extends KeyValueLedgerFactory[InMemoryLedgerBatchingReaderWriter] {

    override def owner(config: Config[Unit], participantConfig: ParticipantConfig)(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[InMemoryLedgerBatchingReaderWriter] =
      new InMemoryLedgerBatchingReaderWriter.Owner(
        config.ledgerId,
        participantConfig.participantId,
        dispatcher = dispatcher,
        state = state,
        materializer = materializer
      )
  }
}
