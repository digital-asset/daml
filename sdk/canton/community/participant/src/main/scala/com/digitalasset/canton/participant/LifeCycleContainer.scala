// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class LifeCycleContainer[T <: AutoCloseable & HasRunOnClosing](
    stateName: String,
    create: () => FutureUnlessShutdown[T],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  // Holds the current state -- will be renewed when initialized again
  // Will NOT be reset to None when closing
  private val stateRef: AtomicReference[Option[T]] = new AtomicReference(None)

  /** Creates a new state, and as it is completed successfully, the subsequent evaluations will
    * return that. As the new state is set, the old state will be closed (if any).
    *
    * @return
    *   The resulting Future will complete after creation of the new state and closure of the old
    *   (if any)
    */
  def initializeNext(): FutureUnlessShutdown[Unit] = {
    noTracingLogger.info(s"Initializing next $stateName")
    for {
      // Rebuild the state value to refresh all the caches
      newValue <- create()
      _ = noTracingLogger.info(s"Next $stateName initialized ($newValue)")
      _ = stateRef.getAndSet(Some(newValue)).foreach { oldValue =>
        // Closing it is a precaution to prevent any usage of the old state.
        if (!oldValue.isClosing) {
          noTracingLogger.info(s"Closing previous $stateName ($oldValue)")
          oldValue.close()
          noTracingLogger.info(s"Closed previous $stateName ($oldValue)")
        } else {
          noTracingLogger.info(s"Previous $stateName ($oldValue) already closed/closing")
        }
      }
    } yield ()
  }

  /** Closes the current state if any. This function returns only after closure is finished. The
    * closed state still can be retrieved after.
    */
  def closeCurrent(): Unit =
    // Close it but don't set the ref to None on purpose here, as the state may still be accessed while the participant
    // is closing.
    stateRef.get().foreach { state =>
      noTracingLogger.info(s"Closing current $stateName ($state)")
      state.close()
      noTracingLogger.info(s"Closed current $stateName ($state)")
    }

  def asEval(implicit traceContext: TraceContext): Eval[T] = Eval.always(
    stateRef.get.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(s"$stateName not initialized")
      )
    )
  )
}
