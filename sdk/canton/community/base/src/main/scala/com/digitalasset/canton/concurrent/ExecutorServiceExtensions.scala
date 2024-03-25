// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.TracedLogger

import java.util.concurrent.{ExecutorService, TimeUnit}

final case class ExecutorServiceExtensions[EC <: ExecutorService](executorService: EC)(
    logger: TracedLogger,
    timeouts: ProcessingTimeout,
) extends AutoCloseable {
  import scala.concurrent.duration.*

  private val DefaultTerminationAwaitDuration: FiniteDuration =
    timeouts.shutdownShort.asFiniteApproximation

  /** Cleanly shuts down an executor service as best we can.
    * @param name Name of the component using the ExecutorService. Used in log messages if executor does not shutdown cleanly.
    */
  def close(name: String): Unit = close(Some(name))

  /** Cleanly shuts down an executor service as best we can.
    */
  override def close(): Unit = close(None)

  private def close(name: Option[String]): Unit = {
    def awaitIdleness(timeout: FiniteDuration): Boolean = executorService match {
      case executor: IdlenessExecutorService => executor.awaitIdleness(timeout)
      case _ => true
    }

    Lifecycle.shutdownResource(
      name.getOrElse(s"executor-${executorService.toString}"),
      () => executorService.shutdown(),
      () => { val _ = executorService.shutdownNow() },
      awaitIdleness,
      timeout => executorService.awaitTermination(timeout.toMillis, TimeUnit.MILLISECONDS),
      DefaultTerminationAwaitDuration,
      DefaultTerminationAwaitDuration,
      logger,
    )
  }
}
