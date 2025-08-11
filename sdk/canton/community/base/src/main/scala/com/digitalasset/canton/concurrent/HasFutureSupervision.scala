// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Mixin-trait for classes that want to run some futures with supervision using a
  * [[FutureSupervisor]] strategy
  */
trait HasFutureSupervision {

  protected def futureSupervisor: FutureSupervisor

  protected def executionContext: ExecutionContext

  protected def supervised[T](description: => String, warnAfter: Duration = 10.seconds)(
      fut: Future[T]
  )(implicit loggingContext: ErrorLoggingContext): Future[T] =
    futureSupervisor.supervised(description, warnAfter)(fut)

  def supervisedUS[T](description: => String, warnAfter: Duration = 10.seconds)(
      fut: FutureUnlessShutdown[T]
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(supervised(description, warnAfter)(fut.unwrap))
}
