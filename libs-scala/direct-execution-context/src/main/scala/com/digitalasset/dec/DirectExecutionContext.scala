// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.dec

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

// Starting from Scala 2.13 this can deleted and replaced by `parasitic`
object DirectExecutionContext extends ExecutionContext {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override final def execute(runnable: Runnable): Unit =
    runnable.run()

  override final def reportFailure(cause: Throwable): Unit =
    logger.error("Unhandled exception", cause)

}
