// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter.Index
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  LedgerFactory,
  ParticipantConfig,
  Runner
}
import com.daml.ledger.participant.state.kvutils.caching
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.resources.{ProgramResource, ResourceOwner}
import scopt.OptionParser

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
      extends LedgerFactory[KeyValueParticipantState, ExtraConfig] {

    override final def readWriteServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
    )(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[KeyValueParticipantState] =
      for {
        readerWriter <- owner(config, participantConfig)
      } yield
        new KeyValueParticipantState(
          readerWriter,
          readerWriter,
          metricRegistry(participantConfig, config))

    def owner(config: Config[ExtraConfig], participantConfig: ParticipantConfig)(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[InMemoryLedgerReaderWriter] =
      new InMemoryLedgerReaderWriter.Owner(
        initialLedgerId = config.ledgerId,
        participantId = participantConfig.participantId,
        metricRegistry = metricRegistry(participantConfig, config),
        stateValueCache = caching.Cache.from(config.stateValueCache),
        dispatcher = dispatcher,
        state = state,
      )

    override val defaultExtraConfig: ExtraConfig = ExtraConfig.default

    override final def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[Int]("maxInboundMessageSize")
        .text(
          s"Max inbound message size in bytes. Defaults to ${ExtraConfig.defaultMaxInboundMessageSize}.")
        .action((maxInboundMessageSize, config) => {
          val extra = config.extra
          config.copy(extra = extra.copy(maxInboundMessageSize = maxInboundMessageSize))
        })
      ()
    }

    override def apiServerConfig(
        participantConfig: ParticipantConfig,
        config: Config[ExtraConfig]): ApiServerConfig =
      super
        .apiServerConfig(participantConfig, config)
        .copy(maxInboundMessageSize = config.extra.maxInboundMessageSize)
  }
}
