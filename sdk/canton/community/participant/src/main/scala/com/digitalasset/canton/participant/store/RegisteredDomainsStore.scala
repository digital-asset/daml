// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbRegisteredDomainsStore
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredDomainsStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait RegisteredDomainsStore extends DomainAliasAndIdStore

/** Keeps track of synchronizerIds of all domains the participant has previously connected to.
  */
trait DomainAliasAndIdStore extends AutoCloseable {

  /** Adds a mapping from a domain alias to a synchronizer id
    */
  def addMapping(alias: DomainAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasAndIdStore.Error, Unit]

  /** Retrieves the current mapping from domain alias to id
    */
  def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): Future[Map[DomainAlias, SynchronizerId]]
}

object DomainAliasAndIdStore {
  trait Error
  final case class DomainAliasAlreadyAdded(alias: DomainAlias, synchronizerId: SynchronizerId)
      extends Error
  final case class SynchronizerIdAlreadyAdded(synchronizerId: SynchronizerId, alias: DomainAlias)
      extends Error
}

object RegisteredDomainsStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): RegisteredDomainsStore = storage match {
    case _: MemoryStorage => new InMemoryRegisteredDomainsStore(loggerFactory)
    case jdbc: DbStorage => new DbRegisteredDomainsStore(jdbc, timeouts, loggerFactory)
  }
}
