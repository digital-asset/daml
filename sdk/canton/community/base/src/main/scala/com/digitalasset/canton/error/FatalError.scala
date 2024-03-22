// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.StackTraceUtil.formatStackTrace

/** When a node encounters a fatal failure that it cannot gracefully handle yet then we exit the process.
  * A process/service monitor such as systemd/k8s will restart the process and the node may recover.
  */
object FatalError {

  private def exitOnFatalError(error: String, exceptionO: Option[Throwable], logger: TracedLogger)(
      implicit traceContext: TraceContext
  ): Nothing = {
    val message =
      s"Fatal error occurred, crashing the node to recover from invalid state: $error\nStack trace:\n${formatStackTrace()}"

    exceptionO match {
      case Some(exception) => logger.error(message, exception)
      case None => logger.error(message)
    }

    sys.exit(1)
  }

  def exitOnFatalError(error: CantonError, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Nothing =
    exitOnFatalError(error.toString, None, logger)

  def exitOnFatalError(message: String, exception: Throwable, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Nothing =
    exitOnFatalError(message, Some(exception), logger)

  def exitOnFatalError(message: String, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Nothing =
    exitOnFatalError(message, None, logger)

}
