// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.api._
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher

object InMemoryLedgerReaderWriter {

  final class BatchingOwner(
      ledgerId: LedgerId,
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      participantId: ParticipantId,
      metrics: Metrics,
      timeProvider: TimeProvider = InMemoryLedgerWriter.DefaultTimeProvider,
      stateValueCache: InMemoryLedgerWriter.StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[KeyValueLedger] {
    override def acquire()(implicit context: ResourceContext): Resource[KeyValueLedger] = {
      val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)
      for {
        writer <- new InMemoryLedgerWriter.BatchingOwner(
          batchingLedgerWriterConfig,
          participantId,
          metrics,
          timeProvider,
          stateValueCache,
          dispatcher,
          state,
          engine,
        ).acquire()
      } yield createKeyValueLedger(reader, writer)
    }
  }

  final class SingleParticipantBatchingOwner(
      ledgerId: LedgerId,
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      participantId: ParticipantId,
      timeProvider: TimeProvider = InMemoryLedgerWriter.DefaultTimeProvider,
      stateValueCache: InMemoryLedgerWriter.StateValueCache = Cache.none,
      metrics: Metrics,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[KeyValueLedger] {
    override def acquire()(implicit context: ResourceContext): Resource[KeyValueLedger] = {
      val state = InMemoryState.empty
      for {
        dispatcher <- dispatcherOwner.acquire()
        readerWriter <- new BatchingOwner(
          ledgerId,
          batchingLedgerWriterConfig,
          participantId,
          metrics,
          timeProvider,
          stateValueCache,
          dispatcher,
          state,
          engine,
        ).acquire()
      } yield readerWriter
    }
  }

  final class PreExecutingOwner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      keySerializationStrategy: StateKeySerializationStrategy,
      metrics: Metrics,
      timeProvider: TimeProvider = InMemoryLedgerWriter.DefaultTimeProvider,
      stateValueCache: InMemoryLedgerWriter.StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[KeyValueLedger] {
    override def acquire()(implicit context: ResourceContext): Resource[KeyValueLedger] = {
      val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)
      for {
        writer <- new InMemoryLedgerWriter.PreExecutingOwner(
          participantId,
          keySerializationStrategy,
          metrics,
          timeProvider,
          stateValueCache,
          dispatcher,
          state,
          engine,
        ).acquire()
      } yield createKeyValueLedger(reader, writer)
    }
  }

}
