// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbRegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredSynchronizersStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait RegisteredSynchronizersStore extends SynchronizerAliasAndIdStore

/** Keeps track of synchronizerIds of all synchronizers the participant has previously connected to.
  *
  * Store invariant:
  *   - For a given synchronizer alias, all the physical synchronizer IDs have the same logical
  *     synchronizer ID
  */
trait SynchronizerAliasAndIdStore extends AutoCloseable {

  /** Adds a mapping from a synchronizer alias to a synchronizer id
    */
  def addMapping(alias: SynchronizerAlias, psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasAndIdStore.Error, Unit]

  /** Retrieves the current mapping from synchronizer alias to id
    */
  def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]]]
}

object SynchronizerAliasAndIdStore {
  sealed trait Error extends Product with Serializable {
    def message: String
  }
  final case class SynchronizerIdAlreadyAdded(
      synchronizerId: PhysicalSynchronizerId,
      existingAlias: SynchronizerAlias,
  ) extends Error {
    val message =
      s"Synchronizer with id $synchronizerId is already registered with alias $existingAlias"
  }

  final case class InconsistentLogicalSynchronizerIds(
      alias: SynchronizerAlias,
      newPSId: PhysicalSynchronizerId,
      existingPSId: PhysicalSynchronizerId,
  ) extends Error {
    val message =
      s"Synchronizer with id $newPSId and alias $alias cannot be registered because existing id `$existingPSId` is for a different logical synchronizer"
  }
}

object RegisteredSynchronizersStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): RegisteredSynchronizersStore = storage match {
    case _: MemoryStorage => new InMemoryRegisteredSynchronizersStore(loggerFactory)
    case jdbc: DbStorage => new DbRegisteredSynchronizersStore(jdbc, timeouts, loggerFactory)
  }
}
