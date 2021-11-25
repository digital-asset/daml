package com.daml.ledger

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

trait TestLoggers {
  val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

}
