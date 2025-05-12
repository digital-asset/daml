// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  ConfigAlreadyExists,
  MissingConfigForSynchronizer,
  UnknownAlias,
}
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.{
  ConfiguredPhysicalSynchronizerId,
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  UnknownPhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.SetParameter

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class DbSynchronizerConnectionConfigStore private[store] (
    override protected val storage: DbStorage,
    releaseProtocolVersion: ReleaseProtocolVersion,
    val aliasResolution: SynchronizerAliasResolution,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit protected val ec: ExecutionContext)
    extends SynchronizerConnectionConfigStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  // Eagerly maintained cache of synchronizer config indexed by SynchronizerAlias
  private val synchronizerConfigCache =
    TrieMap.empty[
      SynchronizerAlias,
      Map[ConfiguredPhysicalSynchronizerId, StoredSynchronizerConnectionConfig],
    ]

  private implicit val setParameterSynchronizerConnectionConfig
      : SetParameter[SynchronizerConnectionConfig] =
    SynchronizerConnectionConfig.getVersionedSetParameter(releaseProtocolVersion.v)

  // Load all configs from the DB into the cache
  private[store] def initialize()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SynchronizerConnectionConfigStore] =
    getAllInternal.map { configs =>
      synchronizerConfigCache
        .addAll(
          configs
            .groupMap(_.config.synchronizerAlias)(config => config.configuredPSId -> config)
            .view
            .mapValues(_.toMap)
        )
        .discard

      this
    }

  private def getInternal(
      key: (SynchronizerAlias, ConfiguredPhysicalSynchronizerId)
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    MissingConfigForSynchronizer,
    StoredSynchronizerConnectionConfig,
  ] = {
    import DbStorage.Implicits.BuilderChain.*

    val (synchronizerAlias, physicalSynchronizerId) = key

    val baseQuery =
      sql"""select config, status from par_synchronizer_connection_configs where synchronizer_alias=$synchronizerAlias and """
    val psidFilter = physicalSynchronizerId.toOption match {
      case Some(psid) => sql"""physical_synchronizer_id=$psid"""
      case None => sql"""physical_synchronizer_id is null"""
    }
    val query = (baseQuery ++ psidFilter)
      .as[(SynchronizerConnectionConfig, SynchronizerConnectionConfigStore.Status)]

    EitherT {
      storage
        .query(
          query.headOption
            .map(_.map { case (config, status) =>
              StoredSynchronizerConnectionConfig(config, status, physicalSynchronizerId)
            }),
          functionFullName,
        )
        .map(_.toRight(MissingConfigForSynchronizer(synchronizerAlias, physicalSynchronizerId)))
    }
  }

  protected def getAllForAliasInternal(
      alias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[StoredSynchronizerConnectionConfig]] = {
    val baseQuery =
      sql"""select config, status, physical_synchronizer_id from par_synchronizer_connection_configs where synchronizer_alias=$alias"""

    val query = baseQuery.as[
      (
          SynchronizerConnectionConfig,
          SynchronizerConnectionConfigStore.Status,
          ConfiguredPhysicalSynchronizerId,
      )
    ]

    storage
      .query(
        query.map(_.map((StoredSynchronizerConnectionConfig.apply _).tupled)),
        functionFullName,
      )
  }

  private def getAllInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[StoredSynchronizerConnectionConfig]] =
    storage.query(
      sql"""select config, status, physical_synchronizer_id from par_synchronizer_connection_configs"""
        .as[
          (
              SynchronizerConnectionConfig,
              SynchronizerConnectionConfigStore.Status,
              ConfiguredPhysicalSynchronizerId,
          )
        ]
        .map(_.map((StoredSynchronizerConnectionConfig.apply _).tupled)),
      functionFullName,
    )

  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    synchronizerConfigCache.clear()
    initialize().map(_ => ())
  }

  override def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConfigAlreadyExists, Unit] = {
    val synchronizerAlias = config.synchronizerAlias

    logger.debug(
      s"Adding connection for ($synchronizerAlias, $configuredPSId) to the store"
    )

    configuredPSId.toOption match {
      case None =>
        getAllFor(synchronizerAlias) match {
          case Right(existingConfigs)
              if existingConfigs.exists(c => c.config == config && c.configuredPSId.isDefined) =>
            // If the connection already exists for a defined synchronizer id, we want to avoid inserting the data again with None
            EitherTUtil.unitUS

          case _ =>
            putInternal(config, status, configuredPSId)
        }

      case Some(_) => putInternal(config, status, configuredPSId)
    }
  }

  private def putInternal(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConfigAlreadyExists, Unit] = {
    val synchronizerAlias = config.synchronizerAlias
    val key = (config.synchronizerAlias, configuredPSId)

    lazy val insertAction: DbAction.WriteOnly[Int] =
      sqlu"""insert
             into par_synchronizer_connection_configs(synchronizer_alias, config, status, physical_synchronizer_id)
             values ($synchronizerAlias, $config, $status, $configuredPSId)
             on conflict do nothing"""

    for {
      nrRows <- EitherT.right(storage.update(insertAction, functionFullName))
      _ <- nrRows match {
        case 1 => EitherTUtil.unitUS[ConfigAlreadyExists]
        case 0 =>
          // If no rows were updated (due to conflict on alias), check if the existing config matches
          EitherT[FutureUnlessShutdown, ConfigAlreadyExists, Unit] {
            getInternal(key)
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
                  ConfigAlreadyExists(synchronizerAlias, configuredPSId),
                )
              }
          }
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
      }
    } yield {
      val storedConfig = StoredSynchronizerConnectionConfig(config, status, configuredPSId)

      // Eagerly update cache
      synchronizerConfigCache
        .updateWith(synchronizerAlias) {
          case Some(existingConfigs) =>
            (existingConfigs + (configuredPSId -> storedConfig)).some
          case None => Map(configuredPSId -> storedConfig).some
        }
        .discard
    }
  }

  override def replace(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForSynchronizer, Unit] = {
    val synchronizerAlias = config.synchronizerAlias

    logger.debug(s"Replacing configuration for ($synchronizerAlias, $configuredPSId)")

    val updateAction = configuredPSId.toOption match {
      case Some(psid) =>
        sqlu"""update par_synchronizer_connection_configs
                set config=$config, physical_synchronizer_id=$configuredPSId
                where synchronizer_alias=$synchronizerAlias and physical_synchronizer_id=$psid"""
      case None =>
        sqlu"""update par_synchronizer_connection_configs
                set config=$config, physical_synchronizer_id=$configuredPSId
                where synchronizer_alias=$synchronizerAlias and physical_synchronizer_id is null"""
    }

    val key = (synchronizerAlias, configuredPSId)

    for {
      // Make sure an existing config exists for the alias
      _ <- getInternal(key)
      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      synchronizerConfigCache
        .updateWith(synchronizerAlias)(
          _.map(_.updatedWith(configuredPSId)(_.map(_.copy(config = config))))
        )
        .discard
    }
  }

  override def get(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig] =
    synchronizerConfigCache
      .get(alias)
      .flatMap(_.get(configuredPSId))
      .toRight(MissingConfigForSynchronizer(alias, configuredPSId))

  override def getAll(): Seq[StoredSynchronizerConnectionConfig] =
    synchronizerConfigCache.values.flatMap(_.values).toSeq

  override def getAllFor(
      alias: SynchronizerAlias
  ): Either[UnknownAlias, NonEmpty[Seq[StoredSynchronizerConnectionConfig]]] =
    synchronizerConfigCache
      .get(alias)
      .map(_.values.toSeq)
      .toRight(UnknownAlias(alias))
      .flatMap(NonEmpty.from(_).toRight(UnknownAlias(alias)))

  def setStatus(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit] = {

    logger.debug(s"Setting status of ($alias, $configuredPSId) to $status")

    val updateAction = configuredPSId.toOption match {
      case Some(psid) =>
        sqlu"""update par_synchronizer_connection_configs
                set status=$status
                where synchronizer_alias=$alias and physical_synchronizer_id=$psid"""
      case None =>
        sqlu"""update par_synchronizer_connection_configs
                set status=$status
                where synchronizer_alias=$alias and physical_synchronizer_id is null"""
    }

    for {
      // Make sure an existing config exists for the alias
      _ <- getInternal((alias, configuredPSId))

      _ <- EitherT.right(storage.update_(updateAction, functionFullName))
    } yield {
      // Eagerly update cache
      synchronizerConfigCache
        .updateWith(alias)(
          _.map(_.updatedWith(configuredPSId)(_.map(_.copy(status = status))))
        )
        .discard
    }
  }

  override def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      physicalSynchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit] = {
    logger.debug(s"Set physical synchronizer id for $alias to $physicalSynchronizerId")

    /*
    Checks whether changes need to be applied to the DB.
    Fails if both (alias, None) (alias, physicalSynchronizerId) are unknown.
     */
    def changeNeeded(): FutureUnlessShutdown[Either[MissingConfigForSynchronizer, Boolean]] = for {
      psidOld <- getInternal((alias, UnknownPhysicalSynchronizerId)).map(_.configuredPSId).value
      psidNew <- getInternal((alias, KnownPhysicalSynchronizerId(physicalSynchronizerId)))
        .map(_.configuredPSId)
        .value
    } yield {
      // Check that there exist one entry for this alias without psid or the change is already applied
      (psidOld, psidNew) match {
        case (Right(_), _) => Right(true)
        case (
              Left(_: MissingConfigForSynchronizer),
              Right(KnownPhysicalSynchronizerId(`physicalSynchronizerId`)),
            ) =>
          Right(false)
        case (Left(_: MissingConfigForSynchronizer), _) =>
          Left(MissingConfigForSynchronizer(alias, UnknownPhysicalSynchronizerId))
      }
    }

    def performChange()
        : EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit] = {
      logger.debug(
        s"Setting physical synchronizer id to $physicalSynchronizerId for synchronizer $alias"
      )

      lazy val updateAction = sqlu"""update par_synchronizer_connection_configs
                              set physical_synchronizer_id=$physicalSynchronizerId
                              where synchronizer_alias=$alias and physical_synchronizer_id is null"""

      for {
        config <- getInternal((alias, UnknownPhysicalSynchronizerId))
        updatedConfig = config.copy(configuredPSId =
          KnownPhysicalSynchronizerId(physicalSynchronizerId)
        )

        _ <- EitherT.right(storage.update_(updateAction, functionFullName))
      } yield {
        synchronizerConfigCache
          .updateWith(alias)(
            _.map(configs =>
              (configs - UnknownPhysicalSynchronizerId + (KnownPhysicalSynchronizerId(
                physicalSynchronizerId
              ) -> updatedConfig))
            )
          )
          .discard
      }
    }

    for {
      isChangeNeeded <- EitherT(changeNeeded()).leftWiden[SynchronizerConnectionConfigStore.Error]

      _ <-
        if (isChangeNeeded)
          performChange()
        else {
          logger.debug(
            s"Physical synchronizer id for $alias is already set to $physicalSynchronizerId"
          )
          EitherTUtil.unitUS[SynchronizerConnectionConfigStore.Error]
        }
    } yield ()
  }
}
