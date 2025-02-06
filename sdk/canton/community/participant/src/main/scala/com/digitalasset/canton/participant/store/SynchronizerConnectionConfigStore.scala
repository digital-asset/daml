// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.db.DbSynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.memory.InMemorySynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

final case class StoredSynchronizerConnectionConfig(
    config: SynchronizerConnectionConfig,
    status: SynchronizerConnectionConfigStore.Status,
)

/** The configured synchronizers and their connection configuration
  */
trait SynchronizerConnectionConfigStore extends AutoCloseable {

  /** Stores a synchronizer connection config together with the status. Primary identifier is the synchronizer alias.
    * Will return an [[SynchronizerConnectionConfigStore.AlreadyAddedForAlias]] error if a config for that alias already exists.
    */
  def put(config: SynchronizerConnectionConfig, status: SynchronizerConnectionConfigStore.Status)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AlreadyAddedForAlias, Unit]

  /** Replaces the config for the given alias.
    * Will return an [[SynchronizerConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def replace(config: SynchronizerConnectionConfig)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForAlias, Unit]

  /** Retrieves the config for a given alias.
    * Will return an [[SynchronizerConnectionConfigStore.MissingConfigForAlias]] error if there is no config for the alias.
    */
  def get(
      alias: SynchronizerAlias
  ): Either[MissingConfigForAlias, StoredSynchronizerConnectionConfig]

  /** Retrieves all configured synchronizers connection configs
    */
  def getAll(): Seq[StoredSynchronizerConnectionConfig]

  /** Dump and refresh all connection configs.
    * Used when a warm participant replica becomes active to ensure it has accurate configs cached.
    */
  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Set the synchronizer configuration status */
  def setStatus(
      source: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForAlias, Unit]

}

object SynchronizerConnectionConfigStore {

  sealed trait Status extends Serializable with Product {
    def dbType: Char
    def canMigrateTo: Boolean
    def canMigrateFrom: Boolean
    def isActive: Boolean
  }

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
  final case class AlreadyAddedForAlias(alias: SynchronizerAlias) extends Error
  final case class MissingConfigForAlias(alias: SynchronizerAlias) extends Error {
    override def toString: String = s"$alias is unknown. Has the synchronizer been registered?"
  }

  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerConnectionConfigStore] =
    storage match {
      case _: MemoryStorage =>
        FutureUnlessShutdown.pure(new InMemorySynchronizerConnectionConfigStore(loggerFactory))
      case dbStorage: DbStorage =>
        new DbSynchronizerConnectionConfigStore(
          dbStorage,
          releaseProtocolVersion,
          timeouts,
          loggerFactory,
        ).initialize()
    }
}
