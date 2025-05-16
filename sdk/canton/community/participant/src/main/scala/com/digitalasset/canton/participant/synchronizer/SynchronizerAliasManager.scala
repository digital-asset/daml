// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*

trait SynchronizerAliasResolution extends AutoCloseable {
  def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId]
  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]
  def aliases: Set[SynchronizerAlias]
}

class SynchronizerAliasManager private (
    synchronizerAliasAndIdStore: SynchronizerAliasAndIdStore,
    initialSynchronizerAliasMap: Map[SynchronizerAlias, SynchronizerId],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with SynchronizerAliasResolution {

  private val synchronizerAliasToId =
    new AtomicReference[BiMap[SynchronizerAlias, SynchronizerId]](
      HashBiMap.create[SynchronizerAlias, SynchronizerId](initialSynchronizerAliasMap.asJava)
    )

  def processHandshake(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasManager.Error, Unit] =
    synchronizerIdForAlias(synchronizerAlias) match {
      /*
       If a synchronizer with this alias is restarted with new id, a different alias should be used to connect to it,
       since it is considered a new synchronizer.
       Since correspondence id <-> alias is per logical id, we use `synchronizerId.logical`.
       */
      case Some(previousId) if previousId != synchronizerId.logical =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          SynchronizerAliasManager.SynchronizerAliasDuplication(
            synchronizerId.logical,
            synchronizerAlias,
            previousId,
          )
        )
      case None => addMapping(synchronizerAlias, synchronizerId.logical)
      case _ => EitherT.rightT[FutureUnlessShutdown, SynchronizerAliasManager.Error](())
    }

  override def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId] = Option(
    synchronizerAliasToId.get().get(alias)
  )
  override def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias] = Option(
    synchronizerAliasToId.get().inverse().get(id)
  )

  /** Return known synchronizer aliases
    *
    * Note: this includes inactive synchronizers! Use
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]] to check the
    * status
    */
  override def aliases: Set[SynchronizerAlias] = Set(
    synchronizerAliasToId.get().keySet().asScala.toSeq*
  )

  /** Return known synchronizer ids
    *
    * Note: this includes inactive synchronizers! Use
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]] to check the
    * status
    */
  def ids: Set[SynchronizerId] = Set(synchronizerAliasToId.get().values().asScala.toSeq*)

  private def addMapping(synchronizerAlias: SynchronizerAlias, synchronizerId: SynchronizerId)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasManager.Error, Unit] =
    for {
      _ <- synchronizerAliasAndIdStore
        .addMapping(synchronizerAlias, synchronizerId)
        .leftMap(error => SynchronizerAliasManager.GenericError(error.toString))
      _ <- EitherT.right[SynchronizerAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- synchronizerAliasAndIdStore.aliasToSynchronizerIdMap.map(map =>
        synchronizerAliasToId.set(HashBiMap.create[SynchronizerAlias, SynchronizerId](map.asJava))
      )
    } yield ()

  override def close(): Unit = LifeCycle.close(synchronizerAliasAndIdStore)(logger)
}

object SynchronizerAliasManager {
  def create(
      synchronizerAliasAndIdStore: SynchronizerAliasAndIdStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerAliasManager] =
    for {
      synchronizerAliasToId <- synchronizerAliasAndIdStore.aliasToSynchronizerIdMap
    } yield new SynchronizerAliasManager(
      synchronizerAliasAndIdStore,
      synchronizerAliasToId,
      loggerFactory,
    )

  sealed trait Error
  final case class GenericError(reason: String) extends Error
  final case class SynchronizerAliasDuplication(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
      previousSynchronizerId: SynchronizerId,
  ) extends Error {
    val message: String =
      s"Will not connect to synchronizer $synchronizerId using alias ${alias.unwrap}. The alias was previously used by another synchronizer $previousSynchronizerId, so please choose a new one."
  }
}
