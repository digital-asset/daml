// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api

import org.slf4j.Logger

object ValidationLogger {
  def logFailure[Request](request: Request, t: Throwable)(implicit logger: Logger): Throwable = {
    logger.debug("Request validation failed for {}", request)
    logger.info(t.getMessage)
    t
  }
}
