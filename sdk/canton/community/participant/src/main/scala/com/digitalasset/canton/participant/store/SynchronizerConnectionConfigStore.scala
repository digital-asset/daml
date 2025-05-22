// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  MissingConfigForSynchronizer,
  NoActiveSynchronizer,
  UnknownAlias,
  UnknownId,
}
import com.digitalasset.canton.participant.store.db.DbSynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.memory.InMemorySynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.{
  ConfiguredPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

final case class StoredSynchronizerConnectionConfig(
    config: SynchronizerConnectionConfig,
    status: SynchronizerConnectionConfigStore.Status,
    configuredPSId: ConfiguredPhysicalSynchronizerId,
)

/** The configured synchronizers and their connection configuration.
  *
  * Upon initial registration, the physical synchronizer id is unknown. Because of that, many
  * methods take an *optional* physical synchronizer id.
  */
trait SynchronizerConnectionConfigStore extends AutoCloseable {
  protected def logger: TracedLogger
  protected implicit def ec: ExecutionContext

  def aliasResolution: SynchronizerAliasResolution

  /** Stores a synchronizer connection config together with the status. Primary identifier is the
    * (synchronizer alias, physical synchronizer id). Will return an
    * [[SynchronizerConnectionConfigStore.ConfigAlreadyExists]] error if a config for that alias and
    * physical synchronizer id already exists.
    */
  def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConfigAlreadyExists, Unit]

  /** Replaces the config for the given alias and physical synchronizer id. Will return an
    * [[SynchronizerConnectionConfigStore.MissingConfigForSynchronizer]] error if there is no config
    * for the (alias, physicalSynchronizerId).
    */
  def replace(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForSynchronizer, Unit]

  def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      physicalSynchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit]

  /** Retrieves the config for a given alias and id. Will return an
    * [[SynchronizerConnectionConfigStore.MissingConfigForSynchronizer]] error if there is no config
    * for the pair (alias, id).
    */
  def get(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig]

  /** Retrieves the active connection for `alias`. Return an
    * [[SynchronizerConnectionConfigStore.Error]] if the alias is unknown or if no connection is
    * active.
    *
    * @param singleExpected
    *   If true, fails if more than one active connection exist.
    */
  def getActive(
      alias: SynchronizerAlias,
      singleExpected: Boolean,
  ): Either[SynchronizerConnectionConfigStore.Error, StoredSynchronizerConnectionConfig] =
    getAllFor(alias).map(_.filter(_.status.isActive)).map(NonEmpty.from).flatMap {
      case None => NoActiveSynchronizer(alias).asLeft
      case Some(configs) =>
        if (configs.sizeIs == 1)
          configs.head1.asRight
        else {
          if (singleExpected)
            AtMostOnePhysicalActive(alias, configs.map(_.configuredPSId).toSet).asLeft
          else
            configs.maxBy1(_.configuredPSId).asRight
        }
    }

  /** Retrieves the active connection for `id`. Return an
    * [[SynchronizerConnectionConfigStore.Error]] if the id is unknown or if no connection is
    * active.
    *
    * @param singleExpected
    *   If true, fails if more than one active connection exist.
    */
  def getActive(
      id: SynchronizerId,
      singleExpected: Boolean,
  ): Either[SynchronizerConnectionConfigStore.Error, StoredSynchronizerConnectionConfig] =
    for {
      alias <- aliasResolution.aliasForSynchronizerId(id).toRight(UnknownId(id))
      config <- getActive(alias, singleExpected = singleExpected)
    } yield config

  /** Retrieves all configured synchronizers connection configs
    */
  def getAll(): Seq[StoredSynchronizerConnectionConfig]

  /*
  Internal method that queries the DB (bypassing the cache for the DB store)
  Exposing as protected to reduce code duplication
   */
  protected def getAllForAliasInternal(
      alias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[StoredSynchronizerConnectionConfig]]

  def getAllFor(
      alias: SynchronizerAlias
  ): Either[UnknownAlias, NonEmpty[Seq[StoredSynchronizerConnectionConfig]]]

  def getAllFor(
      id: SynchronizerId
  ): Either[UnknownId, NonEmpty[Seq[StoredSynchronizerConnectionConfig]]] = for {
    alias <- aliasResolution.aliasForSynchronizerId(id).toRight(UnknownId(id))
    configs <- getAllFor(alias).leftMap(_ => UnknownId(id))
  } yield configs

  def getAllStatusesFor(
      id: SynchronizerId
  ): Either[UnknownId, NonEmpty[Seq[SynchronizerConnectionConfigStore.Status]]] = for {
    alias <- aliasResolution.aliasForSynchronizerId(id).toRight(UnknownId(id))
    configs <- getAllFor(alias).leftMap(_ => UnknownId(id))
  } yield configs.map(_.status)

  /** Dump and refresh all connection configs. Used when a warm participant replica becomes active
    * to ensure it has accurate configs cached.
    */
  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Set the synchronizer configuration status */
  def setStatus(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit]
}

object SynchronizerConnectionConfigStore {

  sealed trait Status extends Serializable with Product with PrettyPrinting {
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
    override protected def pretty: Pretty[Active.type] = prettyOfString(_ => "Active")
  }
  // migrating into
  case object MigratingTo extends Status {
    val dbType: Char = 'M'
    val canMigrateTo: Boolean = true
    val canMigrateFrom: Boolean = false
    val isActive: Boolean = false
    override protected def pretty: Pretty[MigratingTo.type] = prettyOfString(_ => "MigratingTo")
  }
  // migrating off
  case object Vacating extends Status {
    val dbType: Char = 'V'
    val canMigrateTo: Boolean = false
    val canMigrateFrom: Boolean = true
    val isActive: Boolean = false
    override protected def pretty: Pretty[Vacating.type] = prettyOfString(_ => "Vacating")
  }
  case object Inactive extends Status {
    val dbType: Char = 'I'
    val canMigrateTo: Boolean =
      false // we can not downgrade as we might have pruned all important state
    val canMigrateFrom: Boolean = false
    val isActive: Boolean = false
    override protected def pretty: Pretty[Inactive.type] = prettyOfString(_ => "Inactive")
  }

  sealed trait Error extends Serializable with Product {
    def message: String
  }
  final case class AtMostOnePhysicalActive(
      alias: SynchronizerAlias,
      ids: Set[ConfiguredPhysicalSynchronizerId],
  ) extends Error {
    override def message: String =
      s"At most one physical synchronizer should be active for `$alias`. Found: $ids"
  }
  final case class ConfigAlreadyExists(
      alias: SynchronizerAlias,
      id: ConfiguredPhysicalSynchronizerId,
  ) extends Error {
    override def message: String =
      s"Connection for synchronizer with alias `$alias` and id `$id` already exists."
  }
  final case class MissingConfigForSynchronizer(
      alias: SynchronizerAlias,
      id: ConfiguredPhysicalSynchronizerId,
  ) extends Error {
    override def message: String =
      s"Synchronizer with alias `$alias` and id `$id` is unknown. Has the synchronizer been registered?"
  }
  final case class NoActiveSynchronizer(
      alias: SynchronizerAlias
  ) extends Error {
    override def message: String =
      s"No active synchronizer connection found for `$alias`."
  }
  final case class UnknownAlias(
      alias: SynchronizerAlias
  ) extends Error {
    override def message: String =
      s"Synchronizer with alias `$alias` is unknown. Has the synchronizer been registered?"
  }
  final case class UnknownId(id: SynchronizerId) extends Error {
    override def message: String =
      s"Synchronizer with id `$id` is unknown. Has the synchronizer been registered?"
  }

  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      aliasResolution: SynchronizerAliasResolution,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerConnectionConfigStore] =
    storage match {
      case _: MemoryStorage =>
        FutureUnlessShutdown.pure(
          new InMemorySynchronizerConnectionConfigStore(aliasResolution, loggerFactory)
        )
      case dbStorage: DbStorage =>
        new DbSynchronizerConnectionConfigStore(
          dbStorage,
          releaseProtocolVersion,
          aliasResolution,
          timeouts,
          loggerFactory,
        ).initialize()
    }
}
