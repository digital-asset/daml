// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common.util
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object DirectExecutionContext extends ExecutionContext {

  implicit def implicitEC: ExecutionContext = this

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def execute(runnable: Runnable): Unit =
    runnable.run()

  override def reportFailure(cause: Throwable): Unit =
    logger.error("Unhandled exception", cause)
}
