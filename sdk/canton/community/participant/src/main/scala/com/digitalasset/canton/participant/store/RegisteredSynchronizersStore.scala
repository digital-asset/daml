// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbRegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredSynchronizersStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait RegisteredSynchronizersStore extends SynchronizerAliasAndIdStore

/** Keeps track of synchronizerIds of all synchronizers the participant has previously connected to.
  */
trait SynchronizerAliasAndIdStore extends AutoCloseable {

  /** Adds a mapping from a synchronizer alias to a synchronizer id
    */
  def addMapping(alias: SynchronizerAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasAndIdStore.Error, Unit]

  /** Retrieves the current mapping from synchronizer alias to id
    */
  def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, SynchronizerId]]
}

object SynchronizerAliasAndIdStore {
  trait Error
  final case class SynchronizerAliasAlreadyAdded(
      alias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  ) extends Error
  final case class SynchronizerIdAlreadyAdded(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
  ) extends Error
}

object RegisteredSynchronizersStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): RegisteredSynchronizersStore = storage match {
    case _: MemoryStorage => new InMemoryRegisteredSynchronizersStore(loggerFactory)
    case jdbc: DbStorage => new DbRegisteredSynchronizersStore(jdbc, timeouts, loggerFactory)
  }
}
