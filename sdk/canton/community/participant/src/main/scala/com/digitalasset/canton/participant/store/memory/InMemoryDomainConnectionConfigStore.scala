// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  StoredDomainConnectionConfig,
}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class InMemoryDomainConnectionConfigStore(protected override val loggerFactory: NamedLoggerFactory)
    extends DomainConnectionConfigStore
    with NamedLogging {
  private implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

  private val configuredDomainMap =
    new ConcurrentHashMap[DomainAlias, StoredDomainConnectionConfig].asScala

  override def put(
      config: DomainConnectionConfig,
      status: DomainConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, AlreadyAddedForAlias, Unit] =
    EitherT.fromEither[Future](
      configuredDomainMap
        .putIfAbsent(
          config.domain,
          StoredDomainConnectionConfig(config, status),
        )
        .fold[Either[AlreadyAddedForAlias, Unit]](Right(()))(existingConfig =>
          Either.cond(config == existingConfig.config, (), AlreadyAddedForAlias(config.domain))
        )
    )

  override def replace(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, MissingConfigForAlias, Unit] =
    replaceInternal(config.domain, _.copy(config = config))

  private def replaceInternal(
      alias: DomainAlias,
      modifier: StoredDomainConnectionConfig => StoredDomainConnectionConfig,
  ): EitherT[Future, MissingConfigForAlias, Unit] = {
    EitherT.fromEither[Future](
      configuredDomainMap.updateWith(alias)(_.map(modifier)) match {
        case Some(_) =>
          Right(())
        case None =>
          Left(MissingConfigForAlias(alias))
      }
    )
  }

  override def get(
      alias: DomainAlias
  ): Either[MissingConfigForAlias, StoredDomainConnectionConfig] =
    configuredDomainMap.get(alias).toRight(MissingConfigForAlias(alias))

  override def getAll(): Seq[StoredDomainConnectionConfig] =
    configuredDomainMap.values.toSeq

  /** We have no cache so is effectively a noop. */
  override def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  override def close(): Unit = ()

  override def setStatus(
      source: DomainAlias,
      status: DomainConnectionConfigStore.Status,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, MissingConfigForAlias, Unit] =
    replaceInternal(source, _.copy(status = status))

}
