// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.slf4j.LoggerFactory

object ThreadLogger {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  /** Some constant string that makes it easier to filter out corresponding lines from the log */
  private[this] val tag = "[TRACE-THREADS]"

  /** Use this to record what thread a certain code block is executed on */
  def traceThread(method: String): Unit = {
    logger.trace(s"$tag $method")
  }

  /**
    * The Akka .map() stream operator is fused, i.e., it runs on the same actor/thread as the previous operation,
    * see https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html#stream-materialization.
    * Use the following code to record what thead the stream is processed on:
    *
    *     .map(ThreadLogger.traceStreamElement("name"))
    */
  def traceStreamElement[T](method: String)(element: T): T = {
    logger.trace(s"$tag $method")
    element
  }

}
