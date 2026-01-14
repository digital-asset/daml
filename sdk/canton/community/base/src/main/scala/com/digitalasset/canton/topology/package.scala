// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.data.SynchronizerSuccessor
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext

package object topology {
  object LSU {
    final case class Logger private (private val logger: TracedLogger) {
      // For now, all LSU-specific logging should be at info level or higher
      def info(msg: => String)(implicit tc: TraceContext): Unit = logger.info(msg)
      def warn(msg: => String)(implicit tc: TraceContext): Unit = logger.warn(msg)
      def error(msg: => String)(implicit tc: TraceContext): Unit = logger.error(msg)
    }

    object Logger {
      def apply(
          loggerFactory: NamedLoggerFactory,
          klass: Class[?],
          successor: SynchronizerSuccessor,
      ): Logger =
        Logger(loggerFactory.append("lsu", successor.psid.suffix).getTracedLogger(klass))
    }
  }
}
