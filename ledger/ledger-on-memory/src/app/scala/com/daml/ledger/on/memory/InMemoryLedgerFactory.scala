// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.KVOffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  KeyValueReadWriteFactory,
  LedgerFactory,
  ParticipantConfig,
  ReadWriteServiceFactory,
}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher

import scala.concurrent.ExecutionContext

private[memory] class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
    extends LedgerFactory[Unit] {

  override def readWriteServiceFactoryOwner(
      config: Config[Unit],
      participantConfig: ParticipantConfig,
      engine: Engine,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ReadWriteServiceFactory] = {
    val offsetBuilder = new KVOffsetBuilder(version = 0)
    lazy val reader: LedgerReader =
      new InMemoryLedgerReader(config.ledgerId, dispatcher, offsetBuilder, state, metrics)
    new InMemoryLedgerWriter.Owner(
      participantId = participantConfig.participantId,
      keySerializationStrategy = DefaultStateKeySerializationStrategy,
      metrics = metrics,
      timeProvider = InMemoryLedgerWriter.DefaultTimeProvider,
      stateValueCache = caching.WeightedCache.from(
        configuration = config.stateValueCache,
        metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
      ),
      dispatcher = dispatcher,
      offsetBuilder = offsetBuilder,
      state = state,
      engine = engine,
      committerExecutionContext = materializer.executionContext,
    ).map(writer => {
      new KeyValueReadWriteFactory(
        config,
        metrics,
        reader,
        writer,
      )
    })
  }

}
