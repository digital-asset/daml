// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.SetParameter

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DbSynchronizerConnectionConfigStore private[store] (
    override protected val storage: DbStorage,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SynchronizerConnectionConfigStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  // Eagerly maintained cache of synchronizer config indexed by SynchronizerAlias
  private val synchronizerConfigCache =
    TrieMap.empty[SynchronizerAlias, StoredSynchronizerConnectionConfig]

  private implicit val setParameterSynchronizerConnectionConfig
      : SetParameter[SynchronizerConnectionConfig] =
    SynchronizerConnectionConfig.getVersionedSetParameter(releaseProtocolVersion.v)

  // Load all configs from the DB into the cache
  private[store] def initialize()(implicit
      traceContext: TraceContext
  ): Future[SynchronizerConnectionConfigStore] =
    for {
      configs <- getAllInternal
      _ = configs.foreach(s =>
        synchronizerConfigCache
          .put(s.config.synchronizerAlias, s)
          .discard[Option[StoredSynchronizerConnectionConfig]]
      )
    } yield this

  private def getInternal(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    MissingConfigForAlias,
    StoredSynchronizerConnectionConfig,
  ] =
    EitherT {
      storage
        .query(
          sql"""select config, status from par_synchronizer_connection_configs where synchronizer_alias = $synchronizerAlias"""
            .as[(SynchronizerConnectionConfig, SynchronizerConnectionConfigStore.Status)]
            .headOption
            .map(_.map((StoredSynchronizerConnectionConfig.apply _).tupled)),
          functionFullName,
        )
        .map(_.toRight(MissingConfigForAlias(synchronizerAlias)))
    }

  private def getAllInternal(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredSynchronizerConnectionConfig]] =
    storage.query(
      sql"""select config, status from par_synchronizer_connection_configs"""
        .as[(SynchronizerConnectionConfig, SynchronizerConnectionConfigStore.Status)]
        .map(_.map((StoredSynchronizerConnectionConfig.apply _).tupled)),
      functionFullName,
    )

  def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = {
    synchronizerConfigCache.clear()
    initialize().map(_ => ())
  }

  override def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] = {

    val synchronizerAlias = config.synchronizerAlias

    val insertAction: DbAction.WriteOnly[Int] =
      sqlu"""insert
             into par_synchronizer_connection_configs(synchronizer_alias, config, status)
             values ($synchronizerAlias, $config, $status)
             on conflict do nothing"""

    for {
      nrRows <- EitherT.right(storage.update(insertAction, functionFullName))
      _ <- nrRows match {
        case 1 => EitherTUtil.unit[AlreadyAddedForAlias]
        case 0 =>
          // If no rows were updated (due to conflict on alias), check if the existing config matches
          EitherT {
            getInternal(config.synchronizerAlias)
              .valueOr { err =>
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"No existing synchronizer connection config found but failed to insert: $err"
                  )
                )
              }
              .map { existingConfig =>
                Either.cond(
                  existingConfig.config == config,
                  (),
                  AlreadyAddedForAlias(synchronizerAlias),
                )
              }
          }
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
      }
    } yield {
      // Eagerly update cache
      val _ = synchronizerConfigCache.put(
        config.synchronizerAlias,
        StoredSynchronizerConnectionConfig(config, status),
      )
    }
  }

  override def replace(
      config: SynchronizerConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] = {
    val synchronizerAlias = config.synchronizerAlias
    val updateAction = sqlu"""update par_synchronizer_connection_configs
                              set config=$config
                              where synchronizer_alias=$synchronizerAlias"""
    for {
      _ <- getInternal(synchronizerAlias) // Make sure an existing config exists for the alias
      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      synchronizerConfigCache
        .updateWith(config.synchronizerAlias)(_.map(_.copy(config = config)))
        .discard
    }
  }

  override def get(
      alias: SynchronizerAlias
  ): Either[MissingConfigForAlias, StoredSynchronizerConnectionConfig] =
    synchronizerConfigCache.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[StoredSynchronizerConnectionConfig] =
    synchronizerConfigCache.values.toSeq

  def setStatus(
      source: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit] = {
    val updateAction = sqlu"""update par_synchronizer_connection_configs
                              set status=$status
                              where synchronizer_alias=$source"""
    for {
      _ <- getInternal(source) // Make sure an existing config exists for the alias
      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      synchronizerConfigCache.updateWith(source)(_.map(_.copy(status = status))).discard
    }
  }

}
