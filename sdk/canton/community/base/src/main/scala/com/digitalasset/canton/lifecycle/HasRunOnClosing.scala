// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.tracing.TraceContext

trait HasRunOnClosing {

  /** Returns whether the component is closing or has already been closed. No new tasks can be added
    * with [[runOnClose]] during closing.
    */
  def isClosing: Boolean

  /** Schedules the given task to be run upon closing.
    *
    * @return
    *   An [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]] indicates that the task will
    *   have been run when the `LifeCycleManager`'s `closeAsync` method completes or when
    *   `AutoCloseable`'s `close` method returns, unless the returned `LifeCycleRegistrationHandle`
    *   was used to cancel the task or the task has been done beforehand.
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if the task is not
    *   run due to closing. This always happens if [[isClosing]] returns true.
    */
  def runOnClose(task: RunOnClosing): UnlessShutdown[LifeCycleRegistrationHandle]

  /** Register a task to run when closing is initiated, or run it immediately if closing is already
    * ongoing. Unlike [[runOnClose]], this method does not guarantee that this task will have run by
    * the time the `LifeCycleManager`'s `closeAsync` method completes or `AutoCloseable`'s `close`
    * returns. This is because the task is run immediately if the component has already been closed.
    */
  def runOnOrAfterClose(
      task: RunOnClosing
  )(implicit traceContext: TraceContext): LifeCycleRegistrationHandle =
    runOnClose(task).onShutdown {
      runTaskUnlessDone(task)
      LifeCycleManager.DummyHandle
    }

  /** Variant of [[runOnOrAfterClose]] that does not return a
    * [[com.digitalasset.canton.lifecycle.LifeCycleRegistrationHandle]].
    */
  final def runOnOrAfterClose_(task: RunOnClosing)(implicit traceContext: TraceContext): Unit =
    runOnOrAfterClose(task).discard

  protected[this] def runTaskUnlessDone(task: RunOnClosing)(implicit
      traceContext: TraceContext
  ): Unit
}

trait LifeCycleRegistrationHandle {
  def cancel(): Boolean
  def isScheduled: Boolean
}
