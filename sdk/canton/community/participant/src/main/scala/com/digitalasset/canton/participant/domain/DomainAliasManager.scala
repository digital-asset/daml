// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.{
  DomainAliasAndIdStore,
  DomainConnectionConfigStore,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

trait DomainAliasResolution extends AutoCloseable {
  def domainIdForAlias(alias: DomainAlias): Option[DomainId]
  def aliasForDomainId(id: DomainId): Option[DomainAlias]
  def connectionStateForDomain(id: DomainId): Option[DomainConnectionConfigStore.Status]
  def aliases: Set[DomainAlias]
}

class DomainAliasManager private (
    configStore: DomainConnectionConfigStore,
    domainAliasAndIdStore: DomainAliasAndIdStore,
    initialDomainAliasMap: Map[DomainAlias, DomainId],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with DomainAliasResolution {

  private val domainAliasMap =
    new AtomicReference[BiMap[DomainAlias, DomainId]](
      HashBiMap.create[DomainAlias, DomainId](initialDomainAliasMap.asJava)
    )

  def processHandshake(domainAlias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    domainIdForAlias(domainAlias) match {
      // if a domain with this alias is restarted with new id, a different alias should be used to connect to it, since it is considered a new domain
      case Some(previousId) if previousId != domainId =>
        EitherT.leftT[Future, Unit](
          DomainAliasManager.DomainAliasDuplication(domainId, domainAlias, previousId)
        )
      case None => addMapping(domainAlias, domainId)
      case _ => EitherT.rightT[Future, DomainAliasManager.Error](())
    }

  def domainIdForAlias(alias: String): Option[DomainId] =
    DomainAlias.create(alias).toOption.flatMap(al => Option(domainAliasMap.get().get(al)))
  override def domainIdForAlias(alias: DomainAlias): Option[DomainId] = Option(
    domainAliasMap.get().get(alias)
  )
  override def aliasForDomainId(id: DomainId): Option[DomainAlias] = Option(
    domainAliasMap.get().inverse().get(id)
  )

  override def connectionStateForDomain(
      domainId: DomainId
  ): Option[DomainConnectionConfigStore.Status] = for {
    alias <- aliasForDomainId(domainId)
    conf <- configStore.get(alias).toOption
  } yield conf.status

  /** Return known domain aliases
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  override def aliases: Set[DomainAlias] = Set(domainAliasMap.get().keySet().asScala.toSeq*)

  /** Return known domain ids
    *
    * Note: this includes inactive domains! Use [[connectionStateForDomain]] to check the status
    */
  def ids: Set[DomainId] = Set(domainAliasMap.get().values().asScala.toSeq*)

  private def addMapping(domainAlias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainAliasManager.Error, Unit] =
    for {
      _ <- domainAliasAndIdStore
        .addMapping(domainAlias, domainId)
        .leftMap(error => DomainAliasManager.GenericError(error.toString))
      _ <- EitherT.right[DomainAliasManager.Error](updateCaches)
    } yield ()

  private def updateCaches(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- domainAliasAndIdStore.aliasToDomainIdMap.map(map =>
        domainAliasMap.set(HashBiMap.create[DomainAlias, DomainId](map.asJava))
      )
    } yield ()

  override def close(): Unit = Lifecycle.close(domainAliasAndIdStore)(logger)
}

object DomainAliasManager {
  def create(
      configStore: DomainConnectionConfigStore,
      domainAliasAndIdStore: DomainAliasAndIdStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContextExecutor, traceContext: TraceContext): Future[DomainAliasManager] =
    for {
      domainAliasMap <- domainAliasAndIdStore.aliasToDomainIdMap
    } yield new DomainAliasManager(
      configStore,
      domainAliasAndIdStore,
      domainAliasMap,
      loggerFactory,
    )

  sealed trait Error
  final case class GenericError(reason: String) extends Error
  final case class DomainAliasDuplication(
      domainId: DomainId,
      alias: DomainAlias,
      previousDomainId: DomainId,
  ) extends Error {
    val message: String =
      s"Will not connect to domain $domainId using alias ${alias.unwrap}. The alias was previously used by another domain $previousDomainId, so please choose a new one."
  }
}
