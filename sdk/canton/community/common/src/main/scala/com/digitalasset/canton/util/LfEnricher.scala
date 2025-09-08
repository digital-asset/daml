// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.digitalasset.daml.lf.engine.{Engine, Enricher}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value

import scala.concurrent.ExecutionContext

/** Enricher for LF values.
  */
trait LfEnricher {

  def enrichVersionedTransaction(
      versionedTransaction: VersionedTransaction
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, VersionedTransaction]

  def enrichContractInstance(
      contract: FatContractInstance
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, FatContractInstance]

  def enrichContractValue(
      tyCon: Identifier,
      value: Value,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Value]

  def enrichChoiceArgument(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Value]

  def enrichChoiceResult(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Value]

  def enrichContractKey(
      toIdentifier: Identifier,
      value: Value,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Value]

  def enrichView(
      interfaceId: Identifier,
      value: Value,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Value]
}

object LfEnricher {

  def apply(
      engine: Engine,
      forbidLocalContractIds: Boolean,
      packageResolver: PackageResolver,
  ): LfEnricher =
    new LfEnricherImpl(
      new Enricher(
        engine = engine,
        addTrailingNoneFields = false,
        forbidLocalContractIds = forbidLocalContractIds,
      ),
      packageResolver,
    )

  private class LfEnricherImpl(
      delegate: Enricher,
      packageResolver: PackageResolver,
      continueOnInterruption: ContinueOnInterruption = () => true,
  ) extends PackageConsumer(packageResolver, continueOnInterruption)
      with LfEnricher {

    override def enrichVersionedTransaction(
        versionedTransaction: VersionedTransaction
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, VersionedTransaction] =
      consume(delegate.enrichVersionedTransaction(versionedTransaction))

    override def enrichContractInstance(
        contract: FatContractInstance
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, FatContractInstance] =
      consume(delegate.enrichContract(contract))

    override def enrichContractValue(
        tyCon: Identifier,
        value: Value,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Value] =
      consume(delegate.enrichContract(tyCon, value))

    override def enrichChoiceArgument(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Value] =
      consume(delegate.enrichChoiceArgument(toIdentifier, interfaceId, choiceName, unversioned))

    override def enrichChoiceResult(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Value] =
      consume(delegate.enrichChoiceResult(toIdentifier, interfaceId, choiceName, unversioned))

    override def enrichContractKey(
        toIdentifier: Identifier,
        value: Value,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Value] =
      consume(delegate.enrichContractKey(toIdentifier, value))

    override def enrichView(
        interfaceId: Identifier,
        value: Value,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Value] =
      consume(delegate.enrichView(interfaceId, value))

  }

}
