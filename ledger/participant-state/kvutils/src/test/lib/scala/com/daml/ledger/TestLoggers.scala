// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

trait TestLoggers {
  val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

}
