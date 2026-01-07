// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.daml.logging.LoggingContext
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.ExecutionContext

trait LfContractValidation {

  def validate(
      instance: FatContractInstance,
      targetPackageId: LfPackageId,
      contractIdSubstitution: ContractId => ContractId,
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
    new Impl(engine, packageResolver)

  private class Impl(
      delegate: Engine,
      packageResolver: PackageResolver,
      continueOnInterruption: ContinueOnInterruption = () => true,
  ) extends PackageConsumer(packageResolver, continueOnInterruption)
      with LfContractValidation {

    override def validate(
        instance: FatContractInstance,
        targetPackageId: PackageId,
        contractIdSubstitution: ContractId => ContractId,
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
          contractIdSubstitution,
          hashingMethod,
          idValidator = idValidator,
        )
      ).subflatMap(e => e.left.map(_.toString))

  }

}
