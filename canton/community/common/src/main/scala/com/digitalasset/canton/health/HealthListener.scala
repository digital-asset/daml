// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.tracing.TraceContext

/** A listener on the state of a [[HealthElement]] */
trait HealthListener {

  /** Name of the listener. Used for logging. Must not throw. */
  def name: String

  /** Called after the state of a health element has changed.
    *
    * Implementations must be thread-safe in the sense that multiple notifications can run concurrently.
    * Implementations must not block and should not execute significant computations as part of this call.
    * In particular, it is wrong to assume that the listener has finished its updates by the time
    * this method returns.
    *
    * We explicitly do NOT pass along the new state of the health element nor the health element itself.
    * Instead, the listener must query the current state using [[HealthElement.getState]].
    * This ensures that we do not need to synchronize concurrent updates and notifications;
    * the state obtained [[HealthElement.getState]] is guaranteed to be at least as up to date as the notification.
    */
  def poke()(implicit traceContext: TraceContext): Unit
}

object HealthListener {
  def apply(n: String)(onPoke: => Unit): HealthListener = new HealthListener {
    override def name: String = n
    override def poke()(implicit traceContext: TraceContext): Unit = onPoke
  }
}
