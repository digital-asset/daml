// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.kvutils.api._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher

import scala.concurrent.ExecutionContext

object InMemoryLedgerReaderWriter {

  final class Owner(
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      keySerializationStrategy: StateKeySerializationStrategy,
      metrics: Metrics,
      timeProvider: TimeProvider = InMemoryLedgerWriter.DefaultTimeProvider,
      stateValueCache: InMemoryLedgerWriter.StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
      committerExecutionContext: ExecutionContext,
  ) extends ResourceOwner[KeyValueLedger] {
    override def acquire()(implicit context: ResourceContext): Resource[KeyValueLedger] = {
      val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)
      for {
        writer <- new InMemoryLedgerWriter.Owner(
          participantId,
          keySerializationStrategy,
          metrics,
          timeProvider,
          stateValueCache,
          dispatcher,
          state,
          engine,
          committerExecutionContext,
        ).acquire()
      } yield createKeyValueLedger(reader, writer)
    }
  }

}
