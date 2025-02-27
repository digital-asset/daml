// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbSynchronizerParameterStore
import com.digitalasset.canton.participant.store.memory.InMemorySynchronizerParameterStore
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait SynchronizerParameterStore {

  /** Sets new synchronizer parameters. Calls with the same argument are idempotent.
    *
    * @return
    *   The future fails with an [[java.lang.IllegalArgumentException]] if different synchronizer
    *   parameters have been stored before.
    */
  def setParameters(newParameters: StaticSynchronizerParameters)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the last set synchronizer parameters, if any. */
  def lastParameters(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StaticSynchronizerParameters]]
}

object SynchronizerParameterStore {
  def apply(
      storage: Storage,
      synchronizerId: SynchronizerId,
      processingTimeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SynchronizerParameterStore =
    storage match {
      case _: MemoryStorage =>
        new InMemorySynchronizerParameterStore(
        )
      case db: DbStorage =>
        new DbSynchronizerParameterStore(
          synchronizerId,
          db,
          processingTimeouts,
          loggerFactory,
        )
    }
}
