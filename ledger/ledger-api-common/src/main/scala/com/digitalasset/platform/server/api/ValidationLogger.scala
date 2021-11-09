// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api

import com.daml.logging.{ContextualizedLogger, LoggingContext}

object ValidationLogger {
  def logFailure[Request](
      selfServiceErrorCodesEnabled: Boolean
  )(request: Request, t: Throwable)(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): Throwable = {
    logger.debug(s"Request validation failed for $request. Message: ${t.getMessage}")
    if (!selfServiceErrorCodesEnabled) {
      logger.info(t.getMessage)
    }
    t
  }

  def logFailureWithContext[Request, T <: Throwable](
      selfServiceErrorCodesEnabled: Boolean
  )(request: Request, t: T)(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): T = {
    logger.debug(s"Request validation failed for $request. Message: ${t.getMessage}")
    if (!selfServiceErrorCodesEnabled) {
      logger.info(t.getMessage)
    }
    t
  }
}
