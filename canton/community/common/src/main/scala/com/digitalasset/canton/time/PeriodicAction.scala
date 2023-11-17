// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class PeriodicAction(
    clock: Clock,
    interval: NonNegativeFiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
    description: String,
)(check: TraceContext => Future[_])(implicit
    executionContext: ExecutionContext
) extends NamedLogging
    with FlagCloseable {

  TraceContext.withNewTraceContext(setupNextCheck()(_))

  private def runCheck()(implicit traceContext: TraceContext): Unit =
    performUnlessClosingF(s"run-$description")(check(traceContext))
      .onComplete(_ => setupNextCheck())

  private def setupNextCheck()(implicit traceContext: TraceContext): Unit =
    performUnlessClosing(s"setup-$description") {
      val _ = clock.scheduleAfter(_ => runCheck(), interval.duration)
    }.discard

}
