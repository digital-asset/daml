// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import org.slf4j.Logger

object ValidationLogger {
  def logFailure[Request](request: Request, t: Throwable)(implicit logger: Logger): Throwable = {
    logger.debug(s"Request validation failed for $request. Message: ${t.getMessage}")
    logger.info(t.getMessage)
    t
  }

  def logFailureWithContext[Request](request: Request, t: Throwable)(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): Throwable = {
    logger.debug(s"Request validation failed for $request. Message: ${t.getMessage}")
    logger.info(t.getMessage)
    t
  }
}
