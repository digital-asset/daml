// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Mixin-trait for classes that want to run some futures with supervision using a [[FutureSupervisor]] strategy */
trait HasFutureSupervision { this: NamedLogging =>

  protected def futureSupervisor: FutureSupervisor

  protected def executionContext: ExecutionContext

  protected def supervised[T](description: => String, warnAfter: Duration = 10.seconds)(
      fut: Future[T]
  )(implicit traceContext: TraceContext): Future[T] =
    futureSupervisor.supervised(description, warnAfter)(fut)(implicitly, executionContext)

  def supervisedUS[T](description: => String, warnAfter: Duration = 10.seconds)(
      fut: FutureUnlessShutdown[T]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(supervised(description, warnAfter)(fut.unwrap))
}
