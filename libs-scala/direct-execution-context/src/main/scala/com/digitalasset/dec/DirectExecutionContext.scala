// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.dec

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

// TODO Use ContextualizedLogger instead of vanilla SLF4J
class DirectExecutionContext(failureMessage: String) extends ExecutionContext {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override final def execute(runnable: Runnable): Unit =
    runnable.run()

  override final def reportFailure(cause: Throwable): Unit =
    logger.error(failureMessage, cause)

}

object DirectExecutionContext extends DirectExecutionContext("Unhandled exception")
