// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  MissingConfigForSynchronizer,
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
import scala.concurrent.ExecutionContext

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
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConfigAlreadyExists, Unit] =
    EitherT.fromEither[FutureUnlessShutdown](
      configuredSynchronizerMap
        .putIfAbsent(
          (config.synchronizerAlias, configuredPSId),
          StoredSynchronizerConnectionConfig(config, status, configuredPSId),
        )
        .fold(Either.unit[ConfigAlreadyExists])(existingConfig =>
          Either.cond(
            config == existingConfig.config,
            (),
            ConfigAlreadyExists(config.synchronizerAlias, configuredPSId),
          )
        )
    )

  override def replace(
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MissingConfigForSynchronizer, Unit] =
    replaceInternal(config.synchronizerAlias, configuredPSId, _.copy(config = config))

  private def replaceInternal(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
      modifier: StoredSynchronizerConnectionConfig => StoredSynchronizerConnectionConfig,
  ): EitherT[FutureUnlessShutdown, MissingConfigForSynchronizer, Unit] =
    EitherT.fromEither[FutureUnlessShutdown](
      Either.cond(
        configuredSynchronizerMap
          .updateWith((alias, configuredPSId))(_.map(modifier))
          .isDefined,
        (),
        MissingConfigForSynchronizer(alias, configuredPSId),
      )
    )

  override def get(
      alias: SynchronizerAlias,
      configuredPSId: ConfiguredPhysicalSynchronizerId,
  ): Either[MissingConfigForSynchronizer, StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap
      .get((alias, configuredPSId))
      .toRight(MissingConfigForSynchronizer(alias, configuredPSId))

  override def get(
      synchronizerId: PhysicalSynchronizerId
  ): Either[UnknownPSId, StoredSynchronizerConnectionConfig] = {
    val id = KnownPhysicalSynchronizerId(synchronizerId)
    configuredSynchronizerMap
      .collectFirst { case ((_, `id`), config) => config }
      .toRight(UnknownPSId(synchronizerId))
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
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit] = {

    def ensureSinglePhysicalActive(): Either[SynchronizerConnectionConfigStore.Error, Unit] =
      for {
        configs <- getAllFor(alias)

        _ <- configs
          .find(_.configuredPSId == configuredPSId)
          .toRight(MissingConfigForSynchronizer(alias, configuredPSId))

        active = configs.collect {
          case config if config.status.isActive => config.configuredPSId
        }.toSet
        activeNew = active + configuredPSId

        _ <- Either.cond(activeNew.sizeIs == 1, (), AtMostOnePhysicalActive(alias, activeNew))
      } yield ()

    for {
      _ <-
        if (status.isActive) EitherT.fromEither[FutureUnlessShutdown](ensureSinglePhysicalActive())
        else EitherTUtil.unitUS

      _ <- replaceInternal(alias, configuredPSId, _.copy(status = status))
        .leftWiden[SynchronizerConnectionConfigStore.Error]
    } yield ()
  }

  override def setPhysicalSynchronizerId(
      alias: SynchronizerAlias,
      physicalSynchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerConnectionConfigStore.Error, Unit] = {
    /*
    Checks whether changes need to be applied to the DB.
    Fails if both (alias, None) (alias, physicalSynchronizerId) are unknown.
     */
    def changeNeeded(): Either[MissingConfigForSynchronizer, Boolean] = {
      val psidOld = get(alias, UnknownPhysicalSynchronizerId).map(_.configuredPSId)
      val psidNew =
        get(alias, KnownPhysicalSynchronizerId(physicalSynchronizerId)).map(_.configuredPSId)

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

    def performChange() =
      for {
        // Check that there exist one entry for this alias without psid
        config <- EitherT.fromEither[FutureUnlessShutdown](
          get(alias, UnknownPhysicalSynchronizerId)
        )
      } yield {
        configuredSynchronizerMap.addOne(
          (
            (alias, KnownPhysicalSynchronizerId(physicalSynchronizerId)),
            config.copy(configuredPSId = KnownPhysicalSynchronizerId(physicalSynchronizerId)),
          )
        )
        configuredSynchronizerMap.remove((alias, UnknownPhysicalSynchronizerId)).discard

        ()
      }

    for {
      isChangeNeeded <- EitherT
        .fromEither[FutureUnlessShutdown](changeNeeded())
        .leftWiden[SynchronizerConnectionConfigStore.Error]

      _ =
        if (isChangeNeeded)
          performChange()
        else {
          logger.debug(
            s"Physical synchronizer id for $alias is already set to $physicalSynchronizerId"
          )
          EitherTUtil.unitUS
        }
    } yield ()
  }
}
