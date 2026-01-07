// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.LfNodeCreate
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.engine.Engine

import scala.concurrent.ExecutionContext

trait ContractHasher {

  def hash(
      create: LfNodeCreate,
      hashingMethod: Hash.HashingMethod,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Hash]

}

object ContractHasher {

  def apply(engine: Engine, packageResolver: PackageResolver): ContractHasher =
    new Impl(engine, packageResolver)

  private class Impl(
      delegate: Engine,
      packageResolver: PackageResolver,
      continueOnInterruption: ContinueOnInterruption = () => true,
  ) extends PackageConsumer(packageResolver, continueOnInterruption)
      with ContractHasher {
    override def hash(
        create: LfNodeCreate,
        hashingMethod: Hash.HashingMethod,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Hash] =
      consume(delegate.hashCreateNode(create, identity, hashingMethod))
  }

}
