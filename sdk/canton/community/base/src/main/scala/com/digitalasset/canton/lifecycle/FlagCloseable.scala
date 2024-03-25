// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.*
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.Try

/** Adds the [[java.lang.AutoCloseable.close]] method to the interface of [[PerformUnlessClosing]].
  * The component's custom shutdown behaviour should override the `onClosed` method.
  */
trait FlagCloseable extends AutoCloseable with PerformUnlessClosing {
  protected def timeouts: ProcessingTimeout

  override protected def closingTimeout: FiniteDuration = timeouts.closing.asFiniteApproximation

  override final def close(): Unit = super.close()
}

object FlagCloseable {
  def apply(tracedLogger: TracedLogger, timeoutsArgs: ProcessingTimeout): FlagCloseable =
    new FlagCloseable {
      override protected def logger: TracedLogger = tracedLogger
      override protected def timeouts: ProcessingTimeout = timeoutsArgs
    }
}

/** Context to capture and pass through a caller's closing state.
  *
  * This allows us for example to stop operations down the call graph if either the caller or the current component
  * executing an operation is closed.
  */
final case class CloseContext(private val flagCloseable: FlagCloseable) {
  def context: PerformUnlessClosing = flagCloseable
}

object CloseContext {

  /** Combines the 2 given close contexts such that if any of them gets closed,
    * the returned close context is also closed. Works like an OR operator.
    * However if this returned close context is closed directly, the 2 given
    * closed contexts are _NOT_ closed, neither will it wait for any pending
    * tasks on any of the 2 given close context to finish.
    *
    * NOTE: YOU MUST CLOSE THE CONTEXT MANUALLY IN ORDER TO AVOID PILING UP
    *       NEW TASKS ON THE RUNONSHUTDOWN HOOK OF THE PARENT CONTEXTS
    */
  def combineUnsafe(
      closeContext1: CloseContext,
      closeContext2: CloseContext,
      processingTimeout: ProcessingTimeout,
      tracedLogger: TracedLogger,
  )(implicit traceContext: TraceContext): CloseContext = {
    // TODO(#8594) Add a test that this correctly implements the performUnlessClosing semantics
    //  Currently, this is broken because if both closeContext1 and closeContext2 are closed concurrently,
    //  then the close of the created flagCloseable will terminate early for the second call to its close method
    //  and thus not delay that closeContext's closing.
    val flagCloseable = FlagCloseable(tracedLogger, processingTimeout)
    val cancelToken1 = closeContext1.context.runOnShutdown(new RunOnShutdown {
      override def name: String = s"combined-close-ctx1"
      override def done: Boolean =
        closeContext1.context.isClosing && closeContext2.context.isClosing
      override def run(): Unit = flagCloseable.close()
    })
    val cancelToken2 = closeContext2.context.runOnShutdown(new RunOnShutdown {
      override def name: String = s"combined-close-ctx2"
      override def done: Boolean =
        closeContext1.context.isClosing && closeContext2.context.isClosing
      override def run(): Unit = flagCloseable.close()
    })
    flagCloseable.runOnShutdown_(new RunOnShutdown {
      override def name: String = "cancel-close-propagation-of-combined-context"
      override def done: Boolean =
        !closeContext1.context.containsShutdownTask(cancelToken1) &&
          !closeContext2.context.containsShutdownTask(cancelToken2)
      override def run(): Unit = {
        closeContext1.context.cancelShutdownTask(cancelToken1)
        closeContext2.context.cancelShutdownTask(cancelToken2)
      }
    })
    CloseContext(flagCloseable)
  }

  def withCombinedContext[F[_], T](
      closeContext1: CloseContext,
      closeContext2: CloseContext,
      processingTimeout: ProcessingTimeout,
      tracedLogger: TracedLogger,
  )(func: CloseContext => F[T])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): F[T] = {
    val tmp = combineUnsafe(closeContext1, closeContext2, processingTimeout, tracedLogger)
    func(tmp).thereafter(_ => tmp.flagCloseable.close())
  }
}

/** Mix-in to obtain a [[CloseContext]] implicit based on the class's [[FlagCloseable]] */
trait HasCloseContext extends PromiseUnlessShutdownFactory { self: FlagCloseable =>
  implicit val closeContext: CloseContext = CloseContext(self)
}

trait PromiseUnlessShutdownFactory { self: HasCloseContext =>
  protected def logger: TracedLogger

  /** Use this method to create a PromiseUnlessShutdown that will automatically be cancelled when the close context
    * is closed. This allows proper clean up of stray promises when the node is transitioning to a passive state.
    *
    * Note: you should *not* invoke `success` on the returned promise but rather use `trySuccess`. The reason is that
    * the call to `success` may fail in case of shutdown.
    */
  def mkPromise[A](
      description: String,
      futureSupervisor: FutureSupervisor,
      logAfter: Duration = 10.seconds,
      logLevel: Level = Level.DEBUG,
  )(implicit elc: ErrorLoggingContext, ec: ExecutionContext): PromiseUnlessShutdown[A] = {
    val promise = new PromiseUnlessShutdown[A](description, futureSupervisor, logAfter, logLevel)

    val cancelToken = closeContext.context.runOnShutdown(new RunOnShutdown {
      override def name: String = s"$description-abort-promise-on-shutdown"
      override def done: Boolean = promise.isCompleted
      override def run(): Unit = promise.shutdown()
    })(elc.traceContext)

    promise.future
      .onComplete { _ =>
        Try(closeContext.context.cancelShutdownTask(cancelToken)).forFailed(e =>
          logger.debug(s"Failed to cancel shutdown task for $description", e)(elc.traceContext)
        )
      }

    promise
  }
}
