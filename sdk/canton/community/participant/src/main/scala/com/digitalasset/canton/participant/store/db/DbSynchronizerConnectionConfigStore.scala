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
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  Error,
  InconsistentLogicalSynchronizerIds,
  MissingConfigForSynchronizer,
  SynchronizerIdAlreadyAdded,
  UnknownAlias,
  UnknownPSId,
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
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.dbio
import slick.dbio.DBIOAction
import slick.jdbc.{SetParameter, TransactionIsolation}

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

  import com.digitalasset.canton.resource.DbStorage.dbEitherT
  import DbStorage.Implicits.*

  // Eagerly maintained cache of synchronizer config indexed by SynchronizerAlias
  private val synchronizerConfigCache =
    TrieMap.empty[
      SynchronizerAlias,
      Map[ConfiguredPhysicalSynchronizerId, StoredSynchronizerConnectionConfig],
    ]

  private implicit val setParameterSynchronizerConnectionConfig
      : SetParameter[SynchronizerConnectionConfig] =
    SynchronizerConnectionConfig.getVersionedSetParameter(releaseProtocolVersion.v)

  private implicit val setParameterSynchronizerPredecessor
      : SetParameter[Option[SynchronizerPredecessor]] =
    SynchronizerPredecessor.getVersionedSetParameterO(releaseProtocolVersion.v)

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

  private def getInternalQuery(
      key: (SynchronizerAlias, ConfiguredPhysicalSynchronizerId)
  ) = {
    import DbStorage.Implicits.BuilderChain.*

    val (synchronizerAlias, physicalSynchronizerId) = key

    val baseQuery =
      sql"""select config, status, physical_synchronizer_id, synchronizer_predecessor from par_synchronizer_connection_configs where synchronizer_alias=$synchronizerAlias and """
    val psidFilter = physicalSynchronizerId.toOption match {
      case Some(psid) => sql"""physical_synchronizer_id=$psid"""
      case None => sql"""physical_synchronizer_id is null"""
    }
    (baseQuery ++ psidFilter)
      .as[
        (
            SynchronizerConnectionConfig,
            SynchronizerConnectionConfigStore.Status,
            ConfiguredPhysicalSynchronizerId,
            Option[SynchronizerPredecessor],
        )
      ]
  }

  private def getInternal(
      key: (SynchronizerAlias, ConfiguredPhysicalSynchronizerId)
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    Error,
    StoredSynchronizerConnectionConfig,
  ] = {

    val (synchronizerAlias, physicalSynchronizerId) = key
    val query = getInternalQuery(key)

    EitherT {
      storage
        .query(
          query.headOption
            .map(_.map { case (config, status, configuredPSId, predecessor) =>
              StoredSynchronizerConnectionConfig(config, status, configuredPSId, predecessor)
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
          Option[SynchronizerPredecessor],
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
      sql"""select config, status, physical_synchronizer_id, synchronizer_predecessor from par_synchronizer_connection_configs"""
        .as[
          (
              SynchronizerConnectionConfig,
              SynchronizerConnectionConfigStore.Status,
              ConfiguredPhysicalSynchronizerId,
              Option[SynchronizerPredecessor],
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
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    val synchronizerAlias = config.synchronizerAlias

    configuredPSId.toOption match {
      case None =>
        getAllFor(synchronizerAlias) match {
          case Right(existingConfigs)
              if existingConfigs.exists(c => c.config == config && c.configuredPSId.isDefined) =>
            logger.debug(
              s"Not adding connection for ($synchronizerAlias, $configuredPSId) to the store because ($synchronizerAlias, ${existingConfigs
                  .map(_.configuredPSId)}) already exists"
            )

            // If the connection already exists for a defined synchronizer id, we want to avoid inserting the data again with None
            EitherTUtil.unitUS

          case _ =>
            putInternal(config, status, configuredPSId, synchronizerPredecessor)
        }

      case Some(_) => putInternal(config, status, configuredPSId, synchronizerPredecessor)
    }
  }

  // Check that a new PSId is consistent with stored IDs for that alias
  private def checkLogicalIdConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): EitherT[dbio.DBIO, Error, Unit] = for {
    configuredPSIdsForAlias <- dbEitherT[Error](
      sql"select physical_synchronizer_id from par_synchronizer_connection_configs where synchronizer_alias=$alias"
        .as[ConfiguredPhysicalSynchronizerId]
    )

    _ <- EitherT.fromEither[DBIO](
      configuredPSIdsForAlias
        .collectFirst {
          case KnownPhysicalSynchronizerId(existingPSId) if existingPSId.logical != psid.logical =>
            existingPSId
        }
        .map(existing =>
          InconsistentLogicalSynchronizerIds(
            alias = alias,
            newPSId = psid,
            existingPSId = existing,
          )
        )
        .toLeft(())
        .leftWiden[Error]
    )
  } yield ()

  // Ensure this PSId is not already registered with another alias
  private def checkAliasConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): EitherT[dbio.DBIO, Error, Unit] = for {
    existingAliases <- dbEitherT[Error](
      sql"select synchronizer_alias from par_synchronizer_connection_configs where physical_synchronizer_id=$psid"
        .as[SynchronizerAlias]
    )

    _ <- existingAliases.headOption match {
      case Some(`alias`) | None => EitherT.pure[DBIO, Error](())
      case Some(otherAlias) =>
        EitherT.leftT[DBIO, Unit](SynchronizerIdAlreadyAdded(psid, otherAlias): Error)
    }
  } yield ()

  // Ensure there is no other active configuration
  private def checkStatusConsistent(
      psid: ConfiguredPhysicalSynchronizerId,
      alias: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  ): EitherT[dbio.DBIO, Error, Unit] =
    // if the status to be inserted isn't Active, there's no need for further checks
    if (status != SynchronizerConnectionConfigStore.Active) EitherT.pure(())
    else {
      for {
        existingPSId <- dbEitherT[Error](
          sql"select physical_synchronizer_id from par_synchronizer_connection_configs where synchronizer_alias=$alias and status=${SynchronizerConnectionConfigStore.Active}"
            .as[ConfiguredPhysicalSynchronizerId]
            .headOption
        )

        _ <- existingPSId match {
          case Some(`psid`) | None => EitherT.pure[DBIO, Error](())
          case Some(otherConfiguredPSId) =>
            EitherT.leftT[DBIO, Unit](
              AtMostOnePhysicalActive(alias, Set(otherConfiguredPSId, psid)): Error
            )
        }
      } yield ()
    }

  private def putInternal(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    val alias = config.synchronizerAlias
    val key = (config.synchronizerAlias, configuredPSId)

    logger.debug(
      s"Inserting connection for ($alias, $configuredPSId) into the store"
    )

    lazy val insertAction: DbAction.WriteOnly[Int] =
      sqlu"""insert
             into par_synchronizer_connection_configs(synchronizer_alias, config, status, physical_synchronizer_id, synchronizer_predecessor)
             values ($alias, $config, $status, $configuredPSId, $synchronizerPredecessor)
             on conflict do nothing"""

    def checkInsertion(nrRows: Int): EitherT[DBIO, Error, Unit] = nrRows match {
      case 1 => EitherT.pure[DBIO, Error](())
      case 0 =>
        for {
          retrievedResultO <- dbEitherT[Error](getInternalQuery(key)).map(_.headOption)

          _ <- retrievedResultO match {
            case None =>
              EitherT.liftF[DBIO, Error, Unit](
                DBIOAction.failed(
                  new IllegalStateException(
                    s"No existing synchronizer connection config found for alias $alias and id $configuredPSId but failed to insert"
                  )
                )
              )

            case Some((existingConfig, _, _, _)) =>
              EitherT.fromEither[DBIO](
                Either.cond(
                  existingConfig == config,
                  (),
                  ConfigAlreadyExists(alias, configuredPSId): Error,
                )
              )
          }
        } yield ()

      case _ =>
        EitherT.liftF[DBIO, Error, Unit](
          DBIOAction.failed(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
        )
    }

    val queries = for {
      _ <- EitherT.fromEither[DBIO](
        predecessorCompatibilityCheck(configuredPSId, synchronizerPredecessor)
      )

      _ <- configuredPSId match {
        case KnownPhysicalSynchronizerId(psid) =>
          for {
            _ <- checkAliasConsistent(psid, alias)
            _ <- checkLogicalIdConsistent(psid, alias)
          } yield ()

        case UnknownPhysicalSynchronizerId =>
          EitherT.pure[DBIO, Error](())
      }

      _ <- checkStatusConsistent(configuredPSId, alias, status)

      nrRows <- dbEitherT[Error](insertAction)
      _ <- checkInsertion(nrRows)
    } yield ()

    EitherT(
      storage.queryAndUpdate(
        queries.value.transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
        functionFullName,
      )
    ).map { _ =>
      val storedConfig =
        StoredSynchronizerConnectionConfig(config, status, configuredPSId, synchronizerPredecessor)

      // Eagerly update cache
      synchronizerConfigCache
        .updateWith(alias) {
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
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
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

  override def get(
      psid: PhysicalSynchronizerId
  ): Either[UnknownPSId, StoredSynchronizerConnectionConfig] = {
    val id = KnownPhysicalSynchronizerId(psid)

    synchronizerConfigCache.values
      .flatMap(_.get(id))
      .headOption
      .toRight(UnknownPSId(psid))
  }

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

  override def setStatus(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {

    logger.debug(s"Setting status of ($alias, $configuredPSId) to $status")

    val updateAction = for {
      _ <- checkStatusConsistent(configuredPSId, alias, status)
      res <- dbEitherT[Error](configuredPSId.toOption match {
        case Some(psid) =>
          sqlu"""update par_synchronizer_connection_configs
                set status=$status
                where synchronizer_alias=$alias and physical_synchronizer_id=$psid"""
        case None =>
          sqlu"""update par_synchronizer_connection_configs
                set status=$status
                where synchronizer_alias=$alias and physical_synchronizer_id is null"""
      })
    } yield res

    for {
      // Make sure an existing config exists for the alias
      _ <- getInternal((alias, configuredPSId))

      _ <- EitherT(storage.queryAndUpdate(updateAction.value.transactionally, functionFullName))
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
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    logger.debug(s"Set physical synchronizer id for $alias to $psid")

    val queries: EitherT[dbio.DBIO, Error, Unit] = for {
      storedConfigToUpdateO <- getRowToSetPSId(alias, psid)

      _ <- EitherT.fromEither[DBIO](
        predecessorCompatibilityCheck(
          KnownPhysicalSynchronizerId(psid),
          storedConfigToUpdateO.flatMap(_.predecessor),
        )
      )

      _ <- storedConfigToUpdateO match {
        case Some(_) => setPSIdInternal(alias, psid)

        case None =>
          logger.debug(
            s"Physical synchronizer id for $alias is already set to $psid"
          )
          EitherT.pure[DBIO, Error](())
      }
    } yield {
      storedConfigToUpdateO.foreach { storedConfigToUpdate =>
        val updatedConfig =
          storedConfigToUpdate.copy(configuredPSId = KnownPhysicalSynchronizerId(psid))

        synchronizerConfigCache
          .updateWith(alias)(
            _.map(configs =>
              (configs - UnknownPhysicalSynchronizerId +
                (KnownPhysicalSynchronizerId(psid) -> updatedConfig))
            )
          )
      }
    }

    EitherT(
      storage.queryAndUpdate(
        queries.value.transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
        functionFullName,
      )
    )
  }

  /** Performs the PSId update in the DB
    */
  private def setPSIdInternal(
      alias: SynchronizerAlias,
      psid: PhysicalSynchronizerId,
  ): EitherT[dbio.DBIO, Error, Unit] = {

    lazy val updateAction =
      sqlu"""update par_synchronizer_connection_configs
        set physical_synchronizer_id=$psid
        where synchronizer_alias=$alias and physical_synchronizer_id is null"""

    for {
      _ <- checkAliasConsistent(psid, alias)
      _ <- checkLogicalIdConsistent(psid, alias)
      _ <- dbEitherT[Error](updateAction)
    } yield ()
  }

  /** Check whether a row needs to be updated to set the PSId.
    *
    * Fails if both (alias, None) and (alias, physicalSynchronizerId) are unknown. Returns None if
    * PSId is already set, Some(config) if config needs to be updated with PSId
    */
  private def getRowToSetPSId(
      alias: SynchronizerAlias,
      psid: PhysicalSynchronizerId,
  ): EitherT[dbio.DBIO, Error, Option[StoredSynchronizerConnectionConfig]] = {
    def get(id: ConfiguredPhysicalSynchronizerId) =
      dbEitherT[Error](getInternalQuery((alias, id))).map { configs =>
        configs.headOption
          .toRight(MissingConfigForSynchronizer(alias, id))
          .map((StoredSynchronizerConnectionConfig.apply _).tupled)
      }

    for {
      psidOldE <- get(UnknownPhysicalSynchronizerId)
      psidNewE <- get(KnownPhysicalSynchronizerId(psid))

      res <- (psidOldE, psidNewE) match {
        // UnknownPhysicalSynchronizerId is found -> need to set PSId
        case (Right(existingConfig), _) => EitherT.pure[DBIO, Error](existingConfig.some)

        // UnknownPhysicalSynchronizerId is not found, psid is found -> PSId was already set, nothing to do
        case (
              Left(_: MissingConfigForSynchronizer),
              Right(
                StoredSynchronizerConnectionConfig(
                  _,
                  _,
                  KnownPhysicalSynchronizerId(`psid`),
                  _,
                )
              ),
            ) =>
          EitherT.pure[DBIO, Error](None)

        // UnknownPhysicalSynchronizerId is not found, psid is not found -> error
        case (Left(_: MissingConfigForSynchronizer), _) =>
          EitherT.leftT[DBIO, Option[StoredSynchronizerConnectionConfig]](
            MissingConfigForSynchronizer(alias, UnknownPhysicalSynchronizerId): Error
          )
      }
    } yield res
  }
}
