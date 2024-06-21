// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import cats.implicits.catsSyntaxOptionId
import com.digitalasset.daml.lf.engine.EngineLogger
import com.daml.logging.LoggingContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

/** Control logging of Daml `debug` statements
  *
  * @param enabled set to true to enable
  * @param logLevel the log level to use
  * @param matching if non-empty, then only output lines that match one of the given string.
  *                    If the supporting * and ? wildcard characters are used, then the string must match,
  *                    otherwise, the string must be included in the log message.
  */
final case class EngineLoggingConfig(
    enabled: Boolean = true,
    logLevel: Level = Level.DEBUG,
    matching: Seq[String] = Seq.empty,
) {

  private val matcher: String => Boolean =
    if (matching.isEmpty) _ => true
    else {
      val predicates = matching.map { expr =>
        if (expr.contains("*") || expr.contains("?")) {
          val regex = expr.replace("?", ".?").replace("*", ".*?")
          (s: String) => s.matches(regex)
        } else (s: String) => s.contains(expr)
      }
      (s: String) => predicates.exists(_(s))
    }

  def toEngineLogger(loggerFactory: NamedLoggerFactory)(implicit
      traceContext: TraceContext
  ): Option[EngineLogger] =
    if (!enabled) None
    else {
      val logger = loggerFactory.getTracedLogger(EngineLogger.getClass)
      (new EngineLogger {
        override def add(message: String)(implicit
            loggingContext: LoggingContext
        ): Unit = {
          if (matcher(message)) {
            logLevel match {
              case Level.DEBUG => logger.debug(message)
              case Level.INFO => logger.info(message)
              case Level.TRACE => logger.debug(message)
              case Level.WARN => logger.warn(message)
              case Level.ERROR => logger.error(message)
              case _ => ()
            }
          }
        }
      }).some
    }
}
