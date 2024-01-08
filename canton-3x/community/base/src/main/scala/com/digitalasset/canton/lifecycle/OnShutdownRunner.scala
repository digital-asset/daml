// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil.*
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.concurrent.TrieMap
import scala.util.Try

trait OnShutdownRunner { this: AutoCloseable =>

  private val closingFlag: AtomicBoolean = new AtomicBoolean(false)

  private val incrementor: AtomicLong = new AtomicLong(0L)
  private val onShutdownTasks: TrieMap[Long, RunOnShutdown] = TrieMap.empty[Long, RunOnShutdown]

  protected def logger: TracedLogger

  /** Check whether we're closing.
    * Susceptible to race conditions; unless you're using this as a flag to the retry lib or you really know
    * what you're doing, prefer `performUnlessClosing` and friends.
    */
  def isClosing: Boolean = closingFlag.get()

  /** Register a task to run when shutdown is initiated.
    *
    * You can use this for example to register tasks that cancel long-running computations,
    * whose termination you can then wait for in "closeAsync".
    */
  def runOnShutdown_[T](
      task: RunOnShutdown
  )(implicit traceContext: TraceContext): Unit = {
    runOnShutdown(task).discard
  }

  /** Same as [[runOnShutdown_]] but returns a token that allows you to remove the task explicitly from being run
    * using [[cancelShutdownTask]]
    */
  def runOnShutdown[T](
      task: RunOnShutdown
  )(implicit traceContext: TraceContext): Long = {
    val token = incrementor.getAndIncrement()
    onShutdownTasks
      // First remove the tasks that are done
      .filterInPlace { case (_, run) =>
        !run.done
      }
      // Then add the new one
      .put(token, task)
      .discard
    if (isClosing) runOnShutdownTasks()
    token
  }

  /** Removes a shutdown task from the list using a token returned by [[runOnShutdown]]
    */
  def cancelShutdownTask(token: Long): Unit = onShutdownTasks.remove(token).discard
  def containsShutdownTask(token: Long): Boolean = onShutdownTasks.contains(token)

  private def runOnShutdownTasks()(implicit traceContext: TraceContext): Unit = {
    onShutdownTasks.toList.foreach { case (token, task) =>
      Try {
        onShutdownTasks
          .remove(token)
          .filterNot(_.done)
          // TODO(#8594) Time limit the shutdown tasks similar to how we time limit the readers in FlagCloseable
          .foreach(_.run())
      }.forFailed(t => logger.warn(s"Task ${task.name} failed on shutdown!", t))
    }
  }

  @VisibleForTesting
  protected def runStateChanged(waitingState: Boolean = false): Unit = {} // used for unit testing

  protected def onFirstClose(): Unit

  /** Blocks until all earlier tasks have completed and then prevents further tasks from being run.
    */
  protected[this] override def close(): Unit = {
    import TraceContext.Implicits.Empty.*

    val firstCallToClose = closingFlag.compareAndSet(false, true)
    runStateChanged()
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

  /** A closeable container for managing [[RunOnShutdown]] tasks and nothing else. */
  class PureOnShutdownRunner(override protected val logger: TracedLogger)
      extends AutoCloseable
      with OnShutdownRunner {
    override protected def onFirstClose(): Unit = ()
    override def close(): Unit = super.close()
  }
}

/** Trait that can be registered with a [FlagCloseable] to run on shutdown */
trait RunOnShutdown {

  /** the name, used for logging during shutdown */
  def name: String

  /** true if the task has already run (maybe elsewhere) */
  def done: Boolean

  /** invoked by [FlagCloseable] during shutdown */
  def run(): Unit
}
