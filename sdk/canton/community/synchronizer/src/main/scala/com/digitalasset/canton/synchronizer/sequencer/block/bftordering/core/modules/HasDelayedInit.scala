// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules

import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

trait HasDelayedInit[M] { self: NamedLogging =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var initComplete = false
  private val postponedMessages = new mutable.Queue[M]

  /** Called when a module completes relevant initialization and is ready
    * to resume processing events. Any postponed events are dequeued and processed
    * using the provided message handler. For clean startup, we recommend only calling
    * method once per module. However, this method is still idempotent, as subsequent
    * invocations should have no effect.
    */
  protected final def initCompleted(messageHandler: M => Unit)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug("Completing init")
    initComplete = true
    postponedMessages.dequeueAll(_ => true).foreach { message =>
      logger.debug(s"Processing postponed message $message")
      messageHandler(message)
    }
  }

  protected final def ifInitCompleted[T <: M](message: T)(messageHandler: T => Unit)(implicit
      traceContext: TraceContext
  ): Unit =
    if (initComplete) {
      messageHandler(message)
    } else {
      logger.debug(s"Postponing processing message $message because init is still in progress")
      postponedMessages.enqueue(message)
    }
}
