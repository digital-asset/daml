// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, ParticipantConfig}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import scopt.OptionParser

private[memory] class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
    extends LedgerFactory[KeyValueParticipantState, Unit] {

  override final def readWriteServiceOwner(
      config: Config[Unit],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[KeyValueParticipantState] = {
    val metrics = createMetrics(participantConfig, config)
    for {
      readerWriter <- new InMemoryLedgerReaderWriter.Owner(
        ledgerId = config.ledgerId,
        participantId = participantConfig.participantId,
        keySerializationStrategy = DefaultStateKeySerializationStrategy,
        metrics = metrics,
        stateValueCache = caching.WeightedCache.from(
          configuration = config.stateValueCache,
          metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
        ),
        dispatcher = dispatcher,
        state = state,
        engine = engine,
        committerExecutionContext = materializer.executionContext,
      )
    } yield new KeyValueParticipantState(
      readerWriter,
      readerWriter,
      createMetrics(participantConfig, config),
    )
  }

  override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()

  override val defaultExtraConfig: Unit = ()
}
