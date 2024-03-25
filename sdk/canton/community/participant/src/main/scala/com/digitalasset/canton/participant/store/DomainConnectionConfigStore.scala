// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.db.DbDomainConnectionConfigStore
import com.digitalasset.canton.participant.store.memory.InMemoryDomainConnectionConfigStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

final case class StoredDomainConnectionConfig(
    config: DomainConnectionConfig,
    status: DomainConnectionConfigStore.Status,
)

/** The configured domains and their connection configuration
  */
trait DomainConnectionConfigStore extends AutoCloseable {

  /** Stores a domain connection config together with the status. Primary identifier is the domain alias.
    * Will return an [[DomainConnectionConfigStore.AlreadyAddedForAlias]] error if a config for that alias already exists.
    */
  def put(config: DomainConnectionConfig, status: DomainConnectionConfigStore.Status)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AlreadyAddedForAlias, Unit]

  /** Replaces the config for the given alias.
    * Will return an [[DomainConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def replace(config: DomainConnectionConfig)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit]

  /** Retrieves the config for a given alias.
    * Will return an [[DomainConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def get(alias: DomainAlias): Either[MissingConfigForAlias, StoredDomainConnectionConfig]

  /** Retrieves all configured domains connection configs
    */
  def getAll(): Seq[StoredDomainConnectionConfig]

  /** Dump and refresh all connection configs.
    * Used when a warm participant replica becomes active to ensure it has accurate configs cached.
    */
  def refreshCache()(implicit traceContext: TraceContext): Future[Unit]

  /** Set the domain configuration status */
  def setStatus(
      source: DomainAlias,
      status: DomainConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit]

}

object DomainConnectionConfigStore {

  sealed trait Status extends Serializable with Product {
    def dbType: Char
    def canMigrateTo: Boolean
    def canMigrateFrom: Boolean
    def isActive: Boolean
  }

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  implicit val setParameterStatus: SetParameter[Status] = (f, pp) => pp >> f.dbType.toString

  implicit val getResultStatus: GetResult[Status] = GetResult { r =>
    val found = r.nextString()
    Seq(Active, MigratingTo, Vacating, Inactive)
      .find(x => found.headOption.contains(x.dbType))
      .getOrElse(
        throw new DbDeserializationException(s"Failed to deserialize connection status: $found")
      )
  }

  case object Active extends Status {
    val dbType: Char = 'A'
    val canMigrateTo: Boolean = true
    val canMigrateFrom: Boolean = true
    val isActive: Boolean = true
  }
  // migrating into
  case object MigratingTo extends Status {
    val dbType: Char = 'M'
    val canMigrateTo: Boolean = true
    val canMigrateFrom: Boolean = false
    val isActive: Boolean = false
  }
  // migrating off
  case object Vacating extends Status {
    val dbType: Char = 'V'
    val canMigrateTo: Boolean = false
    val canMigrateFrom: Boolean = true
    val isActive: Boolean = false
  }
  case object Inactive extends Status {
    val dbType: Char = 'I'
    val canMigrateTo: Boolean =
      false // we can not downgrade as we might have pruned all important state
    val canMigrateFrom: Boolean = false
    val isActive: Boolean = false
  }

  sealed trait Error extends Serializable with Product
  final case class AlreadyAddedForAlias(alias: DomainAlias) extends Error
  final case class MissingConfigForAlias(alias: DomainAlias) extends Error {
    override def toString: String = s"$alias is unknown. Has the domain been registered?"
  }

  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainConnectionConfigStore] =
    storage match {
      case _: MemoryStorage =>
        Future.successful(new InMemoryDomainConnectionConfigStore(loggerFactory))
      case dbStorage: DbStorage =>
        new DbDomainConnectionConfigStore(
          dbStorage,
          releaseProtocolVersion,
          timeouts,
          loggerFactory,
        ).initialize()
    }
}
