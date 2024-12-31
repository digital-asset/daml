// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.{
  DomainAliasAndIdStore,
  DomainConnectionConfigStore,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

trait DomainAliasResolution extends AutoCloseable {
  def synchronizerIdForAlias(alias: DomainAlias): Option[SynchronizerId]
  def aliasForSynchronizerId(id: SynchronizerId): Option[DomainAlias]
  def connectionStateForDomain(id: SynchronizerId): Option[DomainConnectionConfigStore.Status]
  def aliases: Set[DomainAlias]
}

class DomainAliasManager private (
    configStore: DomainConnectionConfigStore,
    domainAliasAndIdStore: DomainAliasAndIdStore,
    initialDomainAliasMap: Map[DomainAlias, SynchronizerId],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with DomainAliasResolution {

  private val domainAliasMap =
    new AtomicReference[BiMap[DomainAlias, SynchronizerId]](
      HashBiMap.create[DomainAlias, SynchronizerId](initialDomainAliasMap.asJava)
    )

  def processHandshake(domainAlias: DomainAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    synchronizerIdForAlias(domainAlias) match {
      // if a domain with this alias is restarted with new id, a different alias should be used to connect to it, since it is considered a new domain
      case Some(previousId) if previousId != synchronizerId =>
        EitherT.leftT[Future, Unit](
          DomainAliasManager.DomainAliasDuplication(synchronizerId, domainAlias, previousId)
        )
      case None => addMapping(domainAlias, synchronizerId)
      case _ => EitherT.rightT[Future, DomainAliasManager.Error](())
    }

  def synchronizerIdForAlias(alias: String): Option[SynchronizerId] =
    DomainAlias.create(alias).toOption.flatMap(al => Option(domainAliasMap.get().get(al)))
  override def synchronizerIdForAlias(alias: DomainAlias): Option[SynchronizerId] = Option(
    domainAliasMap.get().get(alias)
  )
  override def aliasForSynchronizerId(id: SynchronizerId): Option[DomainAlias] = Option(
    domainAliasMap.get().inverse().get(id)
  )

  override def connectionStateForDomain(
      synchronizerId: SynchronizerId
  ): Option[DomainConnectionConfigStore.Status] = for {
    alias <- aliasForSynchronizerId(synchronizerId)
    conf <- configStore.get(alias).toOption
  } yield conf.status

  /** Return known domain aliases
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  override def aliases: Set[DomainAlias] = Set(domainAliasMap.get().keySet().asScala.toSeq*)

  /** Return known synchronizer ids
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  def ids: Set[SynchronizerId] = Set(domainAliasMap.get().values().asScala.toSeq*)

  private def addMapping(domainAlias: DomainAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    for {
      _ <- domainAliasAndIdStore
        .addMapping(domainAlias, synchronizerId)
        .leftMap(error => DomainAliasManager.GenericError(error.toString))
      _ <- EitherT.right[DomainAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- domainAliasAndIdStore.aliasToSynchronizerIdMap.map(map =>
        domainAliasMap.set(HashBiMap.create[DomainAlias, SynchronizerId](map.asJava))
      )
    } yield ()

  override def close(): Unit = LifeCycle.close(domainAliasAndIdStore)(logger)
}

object DomainAliasManager {
  def create(
      configStore: DomainConnectionConfigStore,
      domainAliasAndIdStore: DomainAliasAndIdStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContextExecutor, traceContext: TraceContext): Future[DomainAliasManager] =
    for {
      domainAliasMap <- domainAliasAndIdStore.aliasToSynchronizerIdMap
    } yield new DomainAliasManager(
      configStore,
      domainAliasAndIdStore,
      domainAliasMap,
      loggerFactory,
    )

  sealed trait Error
  final case class GenericError(reason: String) extends Error
  final case class DomainAliasDuplication(
      synchronizerId: SynchronizerId,
      alias: DomainAlias,
      previousSynchronizerId: SynchronizerId,
  ) extends Error {
    val message: String =
      s"Will not connect to domain $synchronizerId using alias ${alias.unwrap}. The alias was previously used by another domain $previousSynchronizerId, so please choose a new one."
  }
}
