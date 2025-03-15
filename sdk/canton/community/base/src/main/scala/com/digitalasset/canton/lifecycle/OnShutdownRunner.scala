// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil.*
import com.digitalasset.canton.util.TwoPhasePriorityAccumulator

import scala.util.Try

trait OnShutdownRunner extends HasRunOnClosing { this: AutoCloseable =>

  private val onShutdownTasks: TwoPhasePriorityAccumulator[RunOnClosing, Unit] =
    new TwoPhasePriorityAccumulator[RunOnClosing, Unit](Some(_.done))

  protected def logger: TracedLogger

  /** Check whether we're closing. Susceptible to race conditions; unless you're using this as a
    * flag to the retry lib or you really know what you're doing, prefer `performUnlessClosing` and
    * friends.
    */
  override def isClosing: Boolean = !onShutdownTasks.isAccumulating

  override def runOnClose(task: RunOnClosing): UnlessShutdown[LifeCycleRegistrationHandle] =
    onShutdownTasks.accumulate(task, 0) match {
      case Right(handle) => Outcome(new LifeCycleManager.LifeCycleRegistrationHandleImpl(handle))
      case Left(_) => AbortedDueToShutdown
    }

  override protected[this] def runTaskUnlessDone(
      task: RunOnClosing
  )(implicit traceContext: TraceContext): Unit =
    Try {
      // TODO(#8594) Time limit the shutdown tasks similar to how we time limit the readers in FlagCloseable
      if (!task.done) task.run()
    }.forFailed(t => logger.warn(s"Task ${task.name} failed on shutdown!", t))

  private def runOnShutdownTasks()(implicit traceContext: TraceContext): Unit = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    onShutdownTasks.drain().foreach { case (task, _) => runTaskUnlessDone(task) }
  }

  protected def onFirstClose(): Unit

  /** Blocks until all earlier tasks have completed and then prevents further tasks from being run.
    */
  protected[this] override def close(): Unit = {
    import TraceContext.Implicits.Empty.*

    val firstCallToClose = onShutdownTasks.stopAccumulating(()).isEmpty
    if (firstCallToClose) {
      // First run onShutdown tasks.
      // Important to run them in the beginning as they may be used to cancel long-running tasks.
      runOnShutdownTasks()

      onFirstClose()
    } else {
      // TODO(i8594): Ensure we call close only once
    }
  }
}

object OnShutdownRunner {

  /** A closeable container for managing [[RunOnClosing]] tasks and nothing else. */
  class PureOnShutdownRunner(override protected val logger: TracedLogger)
      extends AutoCloseable
      with OnShutdownRunner {
    override protected def onFirstClose(): Unit = ()
    override def close(): Unit = super.close()
  }
}
