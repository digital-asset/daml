// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.apply.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AtMostOnePhysicalActive,
  Error,
  InconsistentPredecessorLogicalSynchronizerIds,
  MissingConfigForSynchronizer,
  NoActiveSynchronizer,
  UnknownAlias,
  UnknownId,
  UnknownPSId,
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

/** @param config
  *   Connection config for the synchronizer
  * @param status
  *   Status of the synchronizer
  * @param configuredPSId
  *   Configured physical synchronizer id. Is unknown before the first connect/handshake is made.
  * @param predecessor
  *   Is defined iff the predecessor exists and the participant was connected to it.
  */
final case class StoredSynchronizerConnectionConfig(
    config: SynchronizerConnectionConfig,
    status: SynchronizerConnectionConfigStore.Status,
    configuredPSId: ConfiguredPhysicalSynchronizerId,
    predecessor: Option[SynchronizerPredecessor],
)

/** The configured synchronizers and their connection configuration.
  *
  * Upon initial registration, the physical synchronizer id is unknown. Because of that, many
  * methods take an *optional* physical synchronizer id.
  *
  * Invariant of the store:
  *   - For a given synchronizer alias, all the configurations have the same logical synchronizer ID
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
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit]

  /** Replaces the config for the given alias and physical synchronizer id. Will return an
    * [[SynchronizerConnectionConfigStore.MissingConfigForSynchronizer]] error if there is no config
    * for the (alias, physicalSynchronizerId).
    */
  def replace(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit]

  def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit]

  /** Retrieves the config for a given alias and id. Will return an
    * [[SynchronizerConnectionConfigStore.MissingConfigForSynchronizer]] error if there is no config
    * for the pair (alias, id).
    */
  def get(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig]

  /** Retrieves the config for a given id. Will return an
    * [[SynchronizerConnectionConfigStore.UnknownPSId]] error if there is no config for id.
    */
  def get(
      psid: PhysicalSynchronizerId
  ): Either[UnknownPSId, StoredSynchronizerConnectionConfig]

  /** Retrieves the active connection for `alias`. Return an
    * [[SynchronizerConnectionConfigStore.Error]] if the alias is unknown or if no connection is
    * active. If several active configs are found, return the one with the highest
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]].
    */
  def getActive(
      alias: SynchronizerAlias
  ): Either[SynchronizerConnectionConfigStore.Error, StoredSynchronizerConnectionConfig] =
    getAllFor(alias).map(_.filter(_.status.isActive)).map(NonEmpty.from).flatMap {
      case None => NoActiveSynchronizer(alias).asLeft
      case Some(configs) =>
        if (configs.sizeIs == 1)
          configs.head1.asRight
        else
          AtMostOnePhysicalActive(alias, configs.map(_.configuredPSId).toSet).asLeft
    }

  /** Retrieves the active connection for `id`. Return an
    * [[SynchronizerConnectionConfigStore.Error]] if the id is unknown or if no connection is
    * active. If several active configs are found, return the one with the highest
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]].
    */
  def getActive(
      id: SynchronizerId
  ): Either[SynchronizerConnectionConfigStore.Error, StoredSynchronizerConnectionConfig] =
    for {
      alias <- aliasResolution.aliasForSynchronizerId(id).toRight(UnknownId(id))
      config <- getActive(alias)
    } yield config

  /** Retrieves all configured synchronizers connection configs
    */
  def getAll(): Seq[StoredSynchronizerConnectionConfig]

  /** Ensures a configured PSId can be the successor of another one.
    */
  protected def predecessorCompatibilityCheck(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  ): Either[Error, Unit] =
    (configuredPSId.toOption, synchronizerPredecessor)
      .mapN((_, _))
      .map { case (psid, SynchronizerPredecessor(predecessorPSId, _)) =>
        Either.cond(
          psid.logical == predecessorPSId.logical,
          (),
          InconsistentPredecessorLogicalSynchronizerIds(
            currentPSId = psid,
            predecessorPSId = predecessorPSId,
          ),
        )
      }
      .getOrElse(Right(()))

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
    Seq(Active, HardMigratingTarget, HardMigratingSource, Inactive, UpgradingTarget)
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

  // Hard migration
  case object HardMigratingSource extends Status {
    val dbType: Char = 'S'
    val canMigrateTo: Boolean = false
    val canMigrateFrom: Boolean = true
    val isActive: Boolean = false
    override protected def pretty: Pretty[HardMigratingSource.type] =
      prettyOfString(_ => "HardMigratingSource")
  }
  case object HardMigratingTarget extends Status {
    val dbType: Char = 'T'
    val canMigrateTo: Boolean = true
    val canMigrateFrom: Boolean = false
    val isActive: Boolean = false
    override protected def pretty: Pretty[HardMigratingTarget.type] =
      prettyOfString(_ => "HardMigratingTarget")
  }

  // For logical synchronizer upgrade
  case object UpgradingTarget extends Status {
    val dbType: Char = 'U'
    val canMigrateTo: Boolean = true
    val canMigrateFrom: Boolean = false

    // inactive so that we connect yet connect to the synchronizer
    val isActive: Boolean = false

    override protected def pretty: Pretty[UpgradingTarget.type] =
      prettyOfString(_ => "UpgradingTarget")
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

  final case class InconsistentLogicalSynchronizerIds(
      alias: SynchronizerAlias,
      newPSId: PhysicalSynchronizerId,
      existingPSId: PhysicalSynchronizerId,
  ) extends Error {
    val message =
      s"Synchronizer with id $newPSId and alias $alias cannot be registered because existing id `$existingPSId` is for a different logical synchronizer"
  }

  final case class InconsistentPredecessorLogicalSynchronizerIds(
      currentPSId: PhysicalSynchronizerId,
      predecessorPSId: PhysicalSynchronizerId,
  ) extends Error {
    val message =
      s"Synchronizer with id $predecessorPSId cannot be the predecessor of $predecessorPSId because their logical IDs are incompatible"
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
  final case class SynchronizerIdAlreadyAdded(
      synchronizerId: PhysicalSynchronizerId,
      existingAlias: SynchronizerAlias,
  ) extends Error {
    val message =
      s"Synchronizer with id $synchronizerId is already registered with alias $existingAlias"
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
  final case class UnknownPSId(id: PhysicalSynchronizerId) extends Error {
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
