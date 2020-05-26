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
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.configuration.LedgerConfiguration
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
        engine: Engine,
    )(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[KeyValueParticipantState] =
      for {
        readerWriter <- owner(config, participantConfig, engine)
      } yield
        new KeyValueParticipantState(
          readerWriter,
          readerWriter,
          createMetrics(participantConfig, config),
        )

    def owner(config: Config[ExtraConfig], participantConfig: ParticipantConfig, engine: Engine)(
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

    override def ledgerConfig(config: Config[ExtraConfig]): LedgerConfiguration =
      LedgerConfiguration.defaultLocalLedger

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
