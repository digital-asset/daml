// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbRegisteredDomainsStore
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredDomainsStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait RegisteredDomainsStore extends DomainAliasAndIdStore

/** Keeps track of domainIds of all domains the participant has previously connected to.
  */
trait DomainAliasAndIdStore extends AutoCloseable {

  /** Adds a mapping from a domain alias to a domain id
    */
  def addMapping(alias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasAndIdStore.Error, Unit]

  /** Retrieves the current mapping from domain alias to id
    */
  def aliasToDomainIdMap(implicit traceContext: TraceContext): Future[Map[DomainAlias, DomainId]]
}

object DomainAliasAndIdStore {
  trait Error
  final case class DomainAliasAlreadyAdded(alias: DomainAlias, domainId: DomainId) extends Error
  final case class DomainIdAlreadyAdded(domainId: DomainId, alias: DomainAlias) extends Error
}

object RegisteredDomainsStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): RegisteredDomainsStore = storage match {
    case _: MemoryStorage => new InMemoryRegisteredDomainsStore(loggerFactory)
    case jdbc: DbStorage => new DbRegisteredDomainsStore(jdbc, timeouts, loggerFactory)
  }
}
