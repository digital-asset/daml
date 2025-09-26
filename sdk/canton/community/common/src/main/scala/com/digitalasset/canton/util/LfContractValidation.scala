// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.daml.logging.LoggingContext
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.LfNodeCreate
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.FatContractInstance

import scala.concurrent.ExecutionContext

trait LfContractHasher {

  def hash(
      create: LfNodeCreate,
      hashingMethod: Hash.HashingMethod,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Hash]

}

trait LfContractValidation extends LfContractHasher {

  def validate(
      instance: FatContractInstance,
      targetPackageId: LfPackageId,
      hashingMethod: Hash.HashingMethod,
      idValidator: Hash => Boolean,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      loggingContext: LoggingContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

}

object LfContractValidation {

  def apply(engine: Engine, packageResolver: PackageResolver): LfContractValidation =
    new LfContractValidationImpl(engine, packageResolver)

  private class LfContractValidationImpl(
      delegate: Engine,
      packageResolver: PackageResolver,
      continueOnInterruption: ContinueOnInterruption = () => true,
  ) extends PackageConsumer(packageResolver, continueOnInterruption)
      with LfContractValidation {

    override def validate(
        instance: FatContractInstance,
        targetPackageId: PackageId,
        hashingMethod: Hash.HashingMethod,
        idValidator: Hash => Boolean,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggingContext: LoggingContext,
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      consume(
        delegate.validateContractInstance(
          instance,
          targetPackageId,
          hashingMethod,
          idValidator = idValidator,
        )
      ).subflatMap(e => e.left.map(_.toString))

    override def hash(
        create: LfNodeCreate,
        hashingMethod: Hash.HashingMethod,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Hash] = {
      consume(delegate.hashCreateNode(create, hashingMethod))
    }
  }

}
