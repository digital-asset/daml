// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
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
import com.digitalasset.canton.topology.{
  ConfiguredPhysicalSynchronizerId,
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  UnknownPhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, blocking}

class InMemorySynchronizerConnectionConfigStore(
    val aliasResolution: SynchronizerAliasResolution,
    protected override val loggerFactory: NamedLoggerFactory,
) extends SynchronizerConnectionConfigStore
    with NamedLogging {
  protected implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

  private val configuredSynchronizerMap = TrieMap[
    (SynchronizerAlias, ConfiguredPhysicalSynchronizerId),
    StoredSynchronizerConnectionConfig,
  ]()

  override def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {

    val alias = config.synchronizerAlias

    val res = blocking {
      synchronized {
        for {
          _ <- predecessorCompatibilityCheck(configuredPSId, synchronizerPredecessor)

          _ <- configuredPSId match {
            case KnownPhysicalSynchronizerId(psid) =>
              for {
                _ <- checkAliasConsistent(psid, alias)
                _ <- checkLogicalIdConsistent(psid, alias)
              } yield ()

            case UnknownPhysicalSynchronizerId => ().asRight
          }

          _ <- checkStatusConsistent(configuredPSId, alias, status)

          _ <- configuredSynchronizerMap
            .putIfAbsent(
              (config.synchronizerAlias, configuredPSId),
              StoredSynchronizerConnectionConfig(
                config,
                status,
                configuredPSId,
                synchronizerPredecessor,
              ),
            )
            .fold(Either.unit[ConfigAlreadyExists])(existingConfig =>
              Either.cond(
                config == existingConfig.config,
                (),
                ConfigAlreadyExists(config.synchronizerAlias, configuredPSId),
              )
            )
        } yield ()
      }
    }

    EitherT.fromEither[FutureUnlessShutdown](res)
  }

  // Ensure there is no other active configuration
  private def checkStatusConsistent(
      psid: ConfiguredPhysicalSynchronizerId,
      alias: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  ): Either[Error, Unit] =
    if (!status.isActive) Either.right(())
    else {
      val existingPSId = configuredSynchronizerMap.collectFirst {
        case ((`alias`, configuredPsid), config) if config.status == Active =>
          configuredPsid
      }
      existingPSId match {
        case Some(`psid`) | None => Either.right(())
        case Some(otherConfiguredPSId) =>
          Either.left(
            AtMostOnePhysicalActive(alias, Set(otherConfiguredPSId, psid)): Error
          )
      }
    }

  // Check that a new PSId is consistent with stored IDs for that alias
  private def checkLogicalIdConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): Either[Error, Unit] = {
    val configuredPsidsForAlias = configuredSynchronizerMap.keySet.collect { case (`alias`, id) =>
      id
    }

    configuredPsidsForAlias
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
  }

  // Ensure this PSId is not already registered with another alias
  private def checkAliasConsistent(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
  ): Either[Error, Unit] =
    configuredSynchronizerMap.keySet
      .collectFirst {
        case (existingAlias, id)
            if id == KnownPhysicalSynchronizerId(psid) && existingAlias != alias =>
          SynchronizerIdAlreadyAdded(psid, existingAlias)
      }
      .toLeft(())

  override def replace(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] =
    EitherT.fromEither(
      replaceInternal(config.synchronizerAlias, configuredPSId, _.copy(config = config))
    )

  private def replaceInternal(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      modifier: StoredSynchronizerConnectionConfig => StoredSynchronizerConnectionConfig,
  ): Either[MissingConfigForSynchronizer, Unit] =
    Either.cond(
      configuredSynchronizerMap
        .updateWith((alias, configuredPSId))(_.map(modifier))
        .isDefined,
      (),
      MissingConfigForSynchronizer(alias, configuredPSId),
    )

  override def get(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap
      .get((alias, configuredPSId))
      .toRight(MissingConfigForSynchronizer(alias, configuredPSId))

  override def get(
      psid: PhysicalSynchronizerId
  ): Either[UnknownPSId, StoredSynchronizerConnectionConfig] = {
    val id = KnownPhysicalSynchronizerId(psid)
    configuredSynchronizerMap
      .collectFirst { case ((_, `id`), config) => config }
      .toRight(UnknownPSId(psid))
  }

  override def getAll(): Seq[StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap.values.toSeq

  /** We have no cache so is effectively a noop. */
  override def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  override def close(): Unit = ()

  override def getAllFor(
      alias: SynchronizerAlias
  ): Either[UnknownAlias, NonEmpty[Seq[StoredSynchronizerConnectionConfig]]] = {
    val connections = configuredSynchronizerMap.collect { case ((`alias`, _), config) =>
      config
    }.toSeq

    if (connections.nonEmpty) NonEmpty.from(connections).toRight(UnknownAlias(alias))
    else UnknownAlias(alias).asLeft
  }

  override protected def getAllForAliasInternal(alias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[StoredSynchronizerConnectionConfig]] =
    FutureUnlessShutdown.pure(getAllFor(alias).map(_.forgetNE).getOrElse(Nil))

  override def setStatus(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {

    val res = blocking {
      synchronized {
        for {
          // ensure that there is an existing config in the store
          _ <- get(alias, configuredPSId)
          // check that there isn't already a different active configuration
          _ <- checkStatusConsistent(configuredPSId, alias, status)
          _ <- replaceInternal(alias, configuredPSId, _.copy(status = status)).leftWiden[Error]
        } yield ()
      }
    }

    EitherT.fromEither(res)
  }

  override def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    /*
    Checks whether changes need to be applied to the DB.
    Fails if both (alias, None) (alias, physicalSynchronizerId) are unknown.
     */
    def changeNeeded(): Either[MissingConfigForSynchronizer, Boolean] = {
      val psidOld = get(alias, UnknownPhysicalSynchronizerId).map(_.configuredPSId)
      val psidNew =
        get(alias, KnownPhysicalSynchronizerId(psid)).map(_.configuredPSId)

      // Check that there exist one entry for this alias without psid or the change is already applied
      (psidOld, psidNew) match {
        case (Right(_), _) => Right(true)
        case (
              Left(_: MissingConfigForSynchronizer),
              Right(KnownPhysicalSynchronizerId(`psid`)),
            ) =>
          Right(false)
        case (Left(_: MissingConfigForSynchronizer), _) =>
          Left(MissingConfigForSynchronizer(alias, UnknownPhysicalSynchronizerId))
      }
    }

    def performChange(): Either[Error, Unit] =
      blocking {
        synchronized {
          for {
            _ <- checkAliasConsistent(psid, alias)
            _ <- checkLogicalIdConsistent(psid, alias)

            // Check that there exist one entry for this alias without psid
            config <- get(alias, UnknownPhysicalSynchronizerId)

            _ <- predecessorCompatibilityCheck(
              KnownPhysicalSynchronizerId(psid),
              config.predecessor,
            )

          } yield {
            configuredSynchronizerMap.addOne(
              (
                (alias, KnownPhysicalSynchronizerId(psid)),
                config.copy(configuredPSId = KnownPhysicalSynchronizerId(psid)),
              )
            )
            configuredSynchronizerMap.remove((alias, UnknownPhysicalSynchronizerId)).discard

            ()
          }
        }
      }

    for {
      isChangeNeeded <- EitherT.fromEither[FutureUnlessShutdown](changeNeeded()).leftWiden[Error]

      _ <-
        if (isChangeNeeded)
          EitherT.fromEither[FutureUnlessShutdown](performChange())
        else {
          logger.debug(
            s"Physical synchronizer id for $alias is already set to $psid"
          )
          EitherTUtil.unitUS[Error]
        }
    } yield ()
  }
}
