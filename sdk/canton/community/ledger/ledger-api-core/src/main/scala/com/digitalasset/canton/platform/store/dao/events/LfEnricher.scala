// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.PackageId as LfPackageId
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.digitalasset.daml.lf.engine as LfEngine
import com.digitalasset.daml.lf.engine.{Engine, Enricher}
import com.digitalasset.daml.lf.value.Value

import scala.concurrent.{ExecutionContext, Future}

/** Enricher for LF values.
  */
trait LfEnricher {

  def enrichContractValue(tyCon: Identifier, value: Value)(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value]

  def enrichChoiceArgument(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value]

  def enrichChoiceResult(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value]

  def enrichContractKey(toIdentifier: Identifier, value: Value)(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value]

  def enrichView(interfaceId: Identifier, value: Value)(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value]
}

object LfEnricher {

  def apply(
      engine: Engine,
      forbidLocalContractIds: Boolean,
      metrics: LedgerApiServerMetrics,
      packageLoader: DeduplicatingPackageLoader,
      loadPackage: (
          LfPackageId,
          LoggingContextWithTrace,
      ) => Future[Option[com.digitalasset.daml.lf.archive.DamlLf.Archive]],
  ): LfEnricher =
    new Impl(
      new Enricher(
        engine = engine,
        addTrailingNoneFields = false,
        forbidLocalContractIds = forbidLocalContractIds,
      ),
      metrics,
      packageLoader,
      loadPackage,
    )

  private class Impl(
      delegate: Enricher,
      metrics: LedgerApiServerMetrics,
      packageLoader: DeduplicatingPackageLoader,
      loadPackage: (
          LfPackageId,
          LoggingContextWithTrace,
      ) => Future[Option[com.digitalasset.daml.lf.archive.DamlLf.Archive]],
  ) extends LfEnricher {

    override def enrichContractValue(tyCon: Identifier, value: Value)(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[Value] =
      consume(delegate.enrichContract(tyCon, value))

    override def enrichChoiceArgument(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[Value] =
      consume(delegate.enrichChoiceArgument(toIdentifier, interfaceId, choiceName, unversioned))

    override def enrichChoiceResult(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[Value] =
      consume(delegate.enrichChoiceResult(toIdentifier, interfaceId, choiceName, unversioned))

    override def enrichContractKey(toIdentifier: Identifier, value: Value)(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[Value] =
      consume(delegate.enrichContractKey(toIdentifier, value))

    override def enrichView(interfaceId: Identifier, value: Value)(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[Value] =
      consume(delegate.enrichView(interfaceId, value))

    private def consume[V](
        result: LfEngine.Result[V]
    )(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContextWithTrace,
    ): Future[V] =
      result match {
        case LfEngine.ResultDone(r) => Future.successful(r)
        case LfEngine.ResultNeedPackage(packageId, resume) =>
          packageLoader
            .loadPackage(
              packageId = packageId,
              delegate = packageId => loadPackage(packageId, loggingContext),
              metric = metrics.index.db.translation.getLfPackage,
            )
            .flatMap(pkgO => consume(resume(pkgO)))
        case LfEngine.ResultError(e) => Future.failed(new RuntimeException(e.message))
        case result =>
          Future.failed(new RuntimeException(s"Unexpected ValueEnricher result: $result"))
      }

  }

}
