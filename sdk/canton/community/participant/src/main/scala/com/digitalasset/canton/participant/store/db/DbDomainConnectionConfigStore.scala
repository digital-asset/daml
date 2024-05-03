// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  StoredDomainConnectionConfig,
}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.SetParameter

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DbDomainConnectionConfigStore private[store] (
    override protected val storage: DbStorage,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DomainConnectionConfigStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  // Eagerly maintained cache of domain config indexed by DomainAlias
  private val domainConfigCache = TrieMap.empty[DomainAlias, StoredDomainConnectionConfig]

  private implicit val setParameterDomainConnectionConfig: SetParameter[DomainConnectionConfig] =
    DomainConnectionConfig.getVersionedSetParameter(releaseProtocolVersion.v)

  // Load all configs from the DB into the cache
  private[store] def initialize()(implicit
      traceContext: TraceContext
  ): Future[DomainConnectionConfigStore] =
    for {
      configs <- getAllInternal
      _ = configs.foreach(s =>
        domainConfigCache.put(s.config.domain, s).discard[Option[StoredDomainConnectionConfig]]
      )
    } yield this

  private def getInternal(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    MissingConfigForAlias,
    StoredDomainConnectionConfig,
  ] =
    EitherT {
      storage
        .query(
          sql"""select config, status from par_domain_connection_configs where domain_alias = $domainAlias"""
            .as[(DomainConnectionConfig, DomainConnectionConfigStore.Status)]
            .headOption
            .map(_.map(StoredDomainConnectionConfig.tupled)),
          functionFullName,
        )
        .map(_.toRight(MissingConfigForAlias(domainAlias)))
    }

  private def getAllInternal(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredDomainConnectionConfig]] =
    storage.query(
      sql"""select config, status from par_domain_connection_configs"""
        .as[(DomainConnectionConfig, DomainConnectionConfigStore.Status)]
        .map(_.map(StoredDomainConnectionConfig.tupled)),
      functionFullName,
    )

  def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = {
    domainConfigCache.clear()
    initialize().map(_ => ())
  }

  override def put(
      config: DomainConnectionConfig,
      status: DomainConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] = {

    val domainAlias = config.domain

    val insertAction: DbAction.WriteOnly[Int] = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( PAR_DOMAIN_CONNECTION_CONFIGS ( domain_alias ) ) */
               into par_domain_connection_configs(domain_alias, config, status)
               values ($domainAlias, $config, $status)"""
      case _ =>
        sqlu"""insert
               into par_domain_connection_configs(domain_alias, config, status)
               values ($domainAlias, $config, $status)
               on conflict do nothing"""
    }

    for {
      nrRows <- EitherT.right(storage.update(insertAction, functionFullName))
      _ <- nrRows match {
        case 1 => EitherTUtil.unit[AlreadyAddedForAlias]
        case 0 =>
          // If no rows were updated (due to conflict on alias), check if the existing config matches
          EitherT {
            getInternal(config.domain)
              .valueOr { err =>
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"No existing domain connection config found but failed to insert: $err"
                  )
                )
              }
              .map { existingConfig =>
                Either.cond(existingConfig.config == config, (), AlreadyAddedForAlias(domainAlias))
              }
          }
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
      }
    } yield {
      // Eagerly update cache
      val _ = domainConfigCache.put(config.domain, StoredDomainConnectionConfig(config, status))
    }
  }

  override def replace(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] = {
    val domainAlias = config.domain
    val updateAction = sqlu"""update par_domain_connection_configs
                              set config=$config
                              where domain_alias=$domainAlias"""
    for {
      _ <- getInternal(domainAlias) // Make sure an existing config exists for the alias
      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      val _ = domainConfigCache.updateWith(config.domain)(_.map(_.copy(config = config)))
    }
  }

  override def get(
      alias: DomainAlias
  ): Either[MissingConfigForAlias, StoredDomainConnectionConfig] =
    domainConfigCache.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[StoredDomainConnectionConfig] = domainConfigCache.values.toSeq

  def setStatus(
      source: DomainAlias,
      status: DomainConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit] = {
    val updateAction = sqlu"""update par_domain_connection_configs
                              set status=$status
                              where domain_alias=$source"""
    for {
      _ <- getInternal(source) // Make sure an existing config exists for the alias
      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      val _ = domainConfigCache.updateWith(source)(_.map(_.copy(status = status)))
    }
  }

}
