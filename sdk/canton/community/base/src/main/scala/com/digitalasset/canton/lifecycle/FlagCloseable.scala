// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.slf4j.event.Level

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

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
    withCloseContext(tracedLogger, timeoutsArgs)

  def withCloseContext(
      tracedLogger: TracedLogger,
      timeoutsArgs: ProcessingTimeout,
  ): FlagCloseable & HasCloseContext =
    new FlagCloseable with HasCloseContext {
      override protected def logger: TracedLogger = tracedLogger
      override protected def timeouts: ProcessingTimeout = timeoutsArgs
    }
}

/** Context to capture and pass through a caller's closing state.
  *
  * This allows us for example to stop operations down the call graph if either the caller or the
  * current component executing an operation is closed.
  */
final case class CloseContext(private val flagCloseable: FlagCloseable) {
  def context: PerformUnlessClosing = flagCloseable
}

object CloseContext {

  /** Combines the 2 given close contexts such that if any of them gets closed, the returned close
    * context is also closed. Works like an OR operator. However if this returned close context is
    * closed directly, the 2 given closed contexts are _NOT_ closed, neither will it wait for any
    * pending tasks on any of the 2 given close context to finish.
    *
    * NOTE: YOU MUST CLOSE THE CONTEXT MANUALLY IN ORDER TO AVOID PILING UP NEW TASKS ON THE
    * RUNONSHUTDOWN HOOK OF THE PARENT CONTEXTS
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
    val cancelToken1 = closeContext1.context.runOnOrAfterClose(new RunOnClosing {
      override def name: String = s"combined-close-ctx1"
      override def done: Boolean =
        closeContext1.context.isClosing && closeContext2.context.isClosing
      override def run()(implicit traceContext: TraceContext): Unit = flagCloseable.close()
    })
    val cancelToken2 = closeContext2.context.runOnOrAfterClose(new RunOnClosing {
      override def name: String = s"combined-close-ctx2"
      override def done: Boolean =
        closeContext1.context.isClosing && closeContext2.context.isClosing
      override def run()(implicit traceContext: TraceContext): Unit = flagCloseable.close()
    })
    flagCloseable.runOnOrAfterClose_(new RunOnClosing {
      override def name: String = "cancel-close-propagation-of-combined-context"
      override def done: Boolean = !cancelToken1.isScheduled && !cancelToken2.isScheduled
      override def run()(implicit traceContext: TraceContext): Unit = {
        cancelToken1.cancel().discard[Boolean]
        cancelToken2.cancel().discard[Boolean]
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

  /** Use this method to create a PromiseUnlessShutdown that will automatically be cancelled when
    * the close context is closed. This allows proper clean up of stray promises when the node is
    * transitioning to a passive state.
    *
    * Note: you should *not* invoke `success` on the returned promise but rather use `trySuccess`.
    * The reason is that the call to `success` may fail in case of shutdown.
    */
  def mkPromise[A](
      description: String,
      futureSupervisor: FutureSupervisor,
      logAfter: Duration = 10.seconds,
      logLevel: Level = Level.DEBUG,
  )(implicit elc: ErrorLoggingContext): PromiseUnlessShutdown[A] =
    PromiseUnlessShutdown
      .abortOnShutdown[A](description, closeContext.context, futureSupervisor, logAfter, logLevel)
}

class DefaultPromiseUnlessShutdownFactory(
    override val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable
    with HasCloseContext
