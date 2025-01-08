// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class InMemorySynchronizerConnectionConfigStore(
    protected override val loggerFactory: NamedLoggerFactory
) extends SynchronizerConnectionConfigStore
    with NamedLogging {
  private implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

  private val configuredSynchronizerMap =
    new ConcurrentHashMap[SynchronizerAlias, StoredSynchronizerConnectionConfig].asScala

  override def put(
      config: SynchronizerConnectionConfig,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] =
    EitherT.fromEither[Future](
      configuredSynchronizerMap
        .putIfAbsent(
          config.synchronizerAlias,
          StoredSynchronizerConnectionConfig(config, status),
        )
        .fold(Either.unit[AlreadyAddedForAlias])(existingConfig =>
          Either.cond(
            config == existingConfig.config,
            (),
            AlreadyAddedForAlias(config.synchronizerAlias),
          )
        )
    )

  override def replace(
      config: SynchronizerConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] =
    replaceInternal(config.synchronizerAlias, _.copy(config = config))

  private def replaceInternal(
      alias: SynchronizerAlias,
      modifier: StoredSynchronizerConnectionConfig => StoredSynchronizerConnectionConfig,
  ): EitherT[Future, MissingConfigForAlias, Unit] =
    EitherT.fromEither[Future](
      Either.cond(
        configuredSynchronizerMap.updateWith(alias)(_.map(modifier)).isDefined,
        (),
        MissingConfigForAlias(alias),
      )
    )

  override def get(
      alias: SynchronizerAlias
  ): Either[MissingConfigForAlias, StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[StoredSynchronizerConnectionConfig] =
    configuredSynchronizerMap.values.toSeq

  /** We have no cache so is effectively a noop. */
  override def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  override def close(): Unit = ()

  override def setStatus(
      source: SynchronizerAlias,
      status: SynchronizerConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit] =
    replaceInternal(source, _.copy(status = status))

}
