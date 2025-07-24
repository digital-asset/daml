// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager.Synchronizers
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*

trait SynchronizerAliasResolution extends AutoCloseable {
  def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId]

  def synchronizerIdsForAlias(
      alias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]]

  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]
  def physicalSynchronizerIds(id: SynchronizerId): Set[PhysicalSynchronizerId]

  def aliases: Set[SynchronizerAlias]
  def physicalSynchronizerIds: Set[PhysicalSynchronizerId]
  def logicalSynchronizerIds: Set[SynchronizerId]
}

class SynchronizerAliasManager private (
    synchronizerAliasAndIdStore: SynchronizerAliasAndIdStore,
    initialSynchronizers: Synchronizers,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with SynchronizerAliasResolution {

  private val synchronizers: AtomicReference[Synchronizers] =
    new AtomicReference[Synchronizers](initialSynchronizers)

  def processHandshake(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasManager.Error, Unit] = {
    val mappingExists =
      synchronizers.get().aliasToPSIds.get(synchronizerAlias).exists(_.contains(synchronizerId))

    if (mappingExists)
      EitherT.rightT[FutureUnlessShutdown, SynchronizerAliasManager.Error](())
    else
      addMapping(synchronizerAlias, synchronizerId)
  }

  override def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId] = Option(
    synchronizers.get.aliasToId.get(alias)
  )

  override def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias] = Option(
    synchronizers.get().aliasToId.inverse().get(id)
  )

  override def physicalSynchronizerIds(id: SynchronizerId): Set[PhysicalSynchronizerId] = {
    val s = synchronizers.get()

    Option(s.aliasToId.inverse().get(id)).map(s.aliasToPSIds).map(_.forgetNE).getOrElse(Set.empty)
  }

  override def synchronizerIdsForAlias(
      alias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]] =
    synchronizers.get.aliasToPSIds.get(alias)

  /** Return known synchronizer aliases
    *
    * Note: this includes inactive synchronizers! Use
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]] to check the
    * status
    */
  override def aliases: Set[SynchronizerAlias] = synchronizers.get().aliasToPSIds.keySet

  /** Return known synchronizer physical ids
    *
    * Note: this includes inactive synchronizers! Use
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]] to check the
    * status
    */
  override def physicalSynchronizerIds: Set[PhysicalSynchronizerId] =
    synchronizers.get().aliasToPSIds.values.flatten.toSet

  /** Return known logical synchronizer ids
    *
    * Note: this includes inactive synchronizers! Use
    * [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore]] to check the
    * status
    */
  override def logicalSynchronizerIds: Set[SynchronizerId] =
    synchronizers.get().aliasToPSIds.values.map(_.map(_.logical)).toSet.flatten

  private def addMapping(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerAliasManager.Error, Unit] =
    for {
      _ <- synchronizerAliasAndIdStore
        .addMapping(synchronizerAlias, synchronizerId)
        .leftMap(error => SynchronizerAliasManager.GenericError(error.message))
      _ <- EitherT.right[SynchronizerAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizerAliasAndIdStore.aliasToSynchronizerIdMap.map { aliasToIds =>
      val synchronizersNew = Synchronizers(aliasToIds)
      synchronizers.set(synchronizersNew)
    }

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
    synchronizerAliasAndIdStore.aliasToSynchronizerIdMap.map { aliasToIds =>
      new SynchronizerAliasManager(
        synchronizerAliasAndIdStore,
        Synchronizers(aliasToIds),
        loggerFactory,
      )
    }

  private final case class Synchronizers(
      aliasToPSIds: Map[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]],
      aliasToId: BiMap[SynchronizerAlias, SynchronizerId],
  )

  private object Synchronizers {
    def apply(
        aliasToIds: Map[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]]
    ): Synchronizers = Synchronizers(
      aliasToPSIds = aliasToIds,
      aliasToId = HashBiMap.create[SynchronizerAlias, SynchronizerId](
        aliasToIds.view.mapValues(_.head1.logical).toMap.asJava
      ),
    )
  }

  sealed trait Error
  final case class GenericError(reason: String) extends Error
}
