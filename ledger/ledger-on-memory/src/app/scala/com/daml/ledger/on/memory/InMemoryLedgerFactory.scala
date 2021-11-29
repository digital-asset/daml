// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, ParticipantConfig}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.participant.state.kvutils.deduplication.CompletionBasedDeduplicationPeriodConverter
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import scopt.OptionParser

import scala.concurrent.ExecutionContext

private[memory] class InMemoryLedgerFactory(dispatcher: Dispatcher[Index], state: InMemoryState)
    extends LedgerFactory[KeyValueParticipantState, Unit] {

  override final def readWriteServiceOwner(
      config: Config[Unit],
      participantConfig: ParticipantConfig,
      engine: Engine,
      completionService: IndexCompletionsService,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[KeyValueParticipantState] = {
    val metrics = createMetrics(participantConfig, config)
    for {
      readerWriter <- new InMemoryLedgerReaderWriter.Owner(
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
        committerExecutionContext = executionContext,
      )
    } yield new KeyValueParticipantState(
      readerWriter,
      readerWriter,
      createMetrics(participantConfig, config),
      config.enableSelfServiceErrorCodes,
      new CompletionBasedDeduplicationPeriodConverter(
        domain.LedgerId(config.ledgerId),
        completionService,
      ),
    )
  }

  override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()

  override val defaultExtraConfig: Unit = ()
}
