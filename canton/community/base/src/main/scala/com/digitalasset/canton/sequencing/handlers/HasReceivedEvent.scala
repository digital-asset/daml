// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.sequencing.SerializedEventHandler

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}

class HasReceivedEvent {

  private val promise = Promise[Unit]()
  private val received = new AtomicBoolean(false)

  def hasReceivedEvent: Boolean = received.get()

  def awaitEvent: Future[Unit] = promise.future
}

/** Capture whether the handler has been supplied an event (not whether it has been successfully processed)
  */
object HasReceivedEvent {
  def apply[E](
      handler: SerializedEventHandler[E]
  ): (HasReceivedEvent, SerializedEventHandler[E]) = {
    val hasReceivedEvent = new HasReceivedEvent

    (
      hasReceivedEvent,
      event => {
        if (!hasReceivedEvent.received.getAndSet(true))
          hasReceivedEvent.promise.success(())
        handler(event)
      },
    )
  }
}
