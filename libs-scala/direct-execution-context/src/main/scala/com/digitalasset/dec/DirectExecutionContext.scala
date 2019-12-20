// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.dec

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

// Starting from Scala 2.13 this can deleted and replaced by `parasitic`
object DirectExecutionContext extends ExecutionContext {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def execute(runnable: Runnable): Unit =
    runnable.run()

  override def reportFailure(cause: Throwable): Unit =
    logger.error("Unhandled exception", cause)
}
