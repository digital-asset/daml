// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.api.KeyValueLedger
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher

private[memory] class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
    extends KeyValueLedgerFactory[KeyValueLedger, Unit] {

  override val defaultExtraConfig: Unit = ()

  override def owner(config: Config[Unit], participantConfig: ParticipantConfig, engine: Engine)(
      implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[KeyValueLedger] = {
    val metrics = createMetrics(participantConfig, config)
    new InMemoryLedgerReaderWriter.Owner(
      ledgerId = config.ledgerId,
      participantId = participantConfig.participantId,
      offsetVersion = 0,
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
  }

}
