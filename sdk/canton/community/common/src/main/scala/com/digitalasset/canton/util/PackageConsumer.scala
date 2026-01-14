// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.{
  Result,
  ResultDone,
  ResultError,
  ResultInterruption,
  ResultNeedPackage,
}
import com.digitalasset.daml.lf.language.Ast.Package

import scala.concurrent.ExecutionContext

object PackageConsumer {
  type PackageResolver = PackageId => TraceContext => FutureUnlessShutdown[Option[Package]]
  type ContinueOnInterruption = () => Boolean
}

abstract class PackageConsumer(
    packageResolver: PackageResolver,
    continueOnInterruption: ContinueOnInterruption,
) {

  private def resolve(packageId: LfPackageId)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Option[Package]] =
    EitherT.right[String].apply(packageResolver(packageId)(traceContext))

  def consume[V](result: Result[V])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, V] =
    result match {
      case ResultError(e) =>
        EitherT.leftT[FutureUnlessShutdown, V](e.toString)

      case ResultNeedPackage(packageId, resume) =>
        for {
          p <- resolve(packageId)
          r <- consume(resume(p))
        } yield r

      case ResultInterruption(continue, abort) =>
        if (continueOnInterruption()) {
          consume(continue())
        } else {
          val reason = abort()
          EitherT.leftT[FutureUnlessShutdown, V](
            s"Aborted engine: ${reason.getOrElse("no context provided")}"
          )
        }

      case ResultDone(result) =>
        EitherT.rightT[FutureUnlessShutdown, String](result)

      case other =>
        EitherT.leftT[FutureUnlessShutdown, V](s"PackageConsumer did not expect: $other")
    }

}
