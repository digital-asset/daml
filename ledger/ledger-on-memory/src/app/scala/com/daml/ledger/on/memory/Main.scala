// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter.Index
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig, Runner}
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.resources.{ProgramResource, ResourceOwner}

object Main {
  def main(args: Array[String]): Unit = {
    val resource = for {
      dispatcher <- InMemoryLedgerReaderWriter.dispatcher
      sharedState = InMemoryState.empty
      factory = new InMemoryLedgerFactory(dispatcher, sharedState)
      runner <- new Runner("In-Memory Ledger", factory).owner(args)
    } yield runner

    new ProgramResource(resource).run()
  }

  class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
      extends KeyValueLedgerFactory[InMemoryLedgerReaderWriter] {
    def owner(config: Config[Unit], participantConfig: ParticipantConfig, engine: Engine)(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[InMemoryLedgerReaderWriter] = {
      val metrics = createMetrics(participantConfig, config)
      new InMemoryLedgerReaderWriter.Owner(
        initialLedgerId = config.ledgerId,
        participantId = participantConfig.participantId,
        metrics = metrics,
        stateValueCache = caching.Cache.from(
          configuration = config.stateValueCache,
          metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
        ),
        dispatcher = dispatcher,
        state = state,
        engine = engine,
      )
    }

    override def ledgerConfig(config: Config[Unit]): LedgerConfiguration =
      LedgerConfiguration.defaultLocalLedger
  }
}
