// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  SynchronizerAliasAndIdStore,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

trait SynchronizerAliasResolution extends AutoCloseable {
  def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId]
  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]
  def connectionStateForDomain(id: SynchronizerId): Option[DomainConnectionConfigStore.Status]
  def aliases: Set[SynchronizerAlias]
}

class SynchronizerAliasManager private (
    configStore: DomainConnectionConfigStore,
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

  def processHandshake(synchronizerAlias: SynchronizerAlias, synchronizerId: SynchronizerId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SynchronizerAliasManager.Error, Unit] =
    synchronizerIdForAlias(synchronizerAlias) match {
      // if a domain with this alias is restarted with new id, a different alias should be used to connect to it, since it is considered a new domain
      case Some(previousId) if previousId != synchronizerId =>
        EitherT.leftT[Future, Unit](
          SynchronizerAliasManager.SynchronizerAliasDuplication(
            synchronizerId,
            synchronizerAlias,
            previousId,
          )
        )
      case None => addMapping(synchronizerAlias, synchronizerId)
      case _ => EitherT.rightT[Future, SynchronizerAliasManager.Error](())
    }

  def synchronizerIdForAlias(alias: String): Option[SynchronizerId] =
    SynchronizerAlias
      .create(alias)
      .toOption
      .flatMap(al => Option(synchronizerAliasToId.get().get(al)))
  override def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId] = Option(
    synchronizerAliasToId.get().get(alias)
  )
  override def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias] = Option(
    synchronizerAliasToId.get().inverse().get(id)
  )

  override def connectionStateForDomain(
      synchronizerId: SynchronizerId
  ): Option[DomainConnectionConfigStore.Status] = for {
    alias <- aliasForSynchronizerId(synchronizerId)
    conf <- configStore.get(alias).toOption
  } yield conf.status

  /** Return known synchronizer aliases
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  override def aliases: Set[SynchronizerAlias] = Set(
    synchronizerAliasToId.get().keySet().asScala.toSeq*
  )

  /** Return known synchronizer ids
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  def ids: Set[SynchronizerId] = Set(synchronizerAliasToId.get().values().asScala.toSeq*)

  private def addMapping(synchronizerAlias: SynchronizerAlias, synchronizerId: SynchronizerId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SynchronizerAliasManager.Error, Unit] =
    for {
      _ <- synchronizerAliasAndIdStore
        .addMapping(synchronizerAlias, synchronizerId)
        .leftMap(error => SynchronizerAliasManager.GenericError(error.toString))
      _ <- EitherT.right[SynchronizerAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- synchronizerAliasAndIdStore.aliasToSynchronizerIdMap.map(map =>
        synchronizerAliasToId.set(HashBiMap.create[SynchronizerAlias, SynchronizerId](map.asJava))
      )
    } yield ()

  override def close(): Unit = LifeCycle.close(synchronizerAliasAndIdStore)(logger)
}

object SynchronizerAliasManager {
  def create(
      configStore: DomainConnectionConfigStore,
      synchronizerAliasAndIdStore: SynchronizerAliasAndIdStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): Future[SynchronizerAliasManager] =
    for {
      synchronizerAliasToId <- synchronizerAliasAndIdStore.aliasToSynchronizerIdMap
    } yield new SynchronizerAliasManager(
      configStore,
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
      s"Will not connect to domain $synchronizerId using alias ${alias.unwrap}. The alias was previously used by another domain $previousSynchronizerId, so please choose a new one."
  }
}
