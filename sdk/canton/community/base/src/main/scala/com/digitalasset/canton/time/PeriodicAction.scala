// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class PeriodicAction(
    clock: Clock,
    interval: NonNegativeFiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
    description: String,
)(check: TraceContext => FutureUnlessShutdown[_])(implicit
    executionContext: ExecutionContext
) extends NamedLogging
    with FlagCloseable {

  TraceContext.withNewTraceContext(setupNextCheck()(_))

  private def runCheck()(implicit traceContext: TraceContext): Unit =
    performUnlessClosingUSF(s"run-$description")(check(traceContext))
      .onComplete(_ => setupNextCheck())

  private def setupNextCheck()(implicit traceContext: TraceContext): Unit =
    performUnlessClosing(s"setup-$description") {
      val _ = clock.scheduleAfter(_ => runCheck(), interval.duration)
    }.discard

}
