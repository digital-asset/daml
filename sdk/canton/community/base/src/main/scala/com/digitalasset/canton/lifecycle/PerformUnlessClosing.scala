// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Provides a way to synchronize closing with other running tasks in the class, such that new tasks
  * aren't scheduled while closing, and such that closing waits for the scheduled tasks.
  *
  * Use this type to pass such synchronization objects to other objects that merely need to
  * synchronize, but should not be able to initiate closing themselves. To that end, this trait does
  * not expose the [[java.lang.AutoCloseable.close]] method.
  *
  * @see
  *   FlagCloseable does expose the [[java.lang.AutoCloseable.close]] method.
  */
trait PerformUnlessClosing extends OnShutdownRunner with HasSynchronizeWithReaders {
  this: AutoCloseable =>
  protected def closingTimeout: FiniteDuration

  /** Set this to true to get detailed information about all futures that did not complete during
    * shutdown.
    */
  override protected[this] def keepTrackOfReaderCallStack: Boolean = false
  override protected[this] def synchronizeWithClosingPatience: FiniteDuration = closingTimeout

  override protected[this] def nameInternal: String = this.getClass.getSimpleName

  override def synchronizeWithClosingUS[F[_], A](
      name: String
  )(f: => F[A])(implicit traceContext: TraceContext, F: Thereafter[F]): UnlessShutdown[F[A]] =
    this.withReader(name)(f).tapOnShutdown {
      logger.debug(s"Won't schedule the task '$name' as this object is closing")
    }

  /** Convenience method for synchronizing on `Future`s instead of `FutureUnlessShutdown`s.
    * Equivalent to
    * {{{
    *   synchronizeWithClosing(name)(FutureUnlessShutdown.outcomeF(f))
    * }}}
    */
  def synchronizeWithClosingF[A](name: String)(
      f: => Future[A]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    synchronizeWithClosing(name)(FutureUnlessShutdown.outcomeF(f))

  protected def onClosed(): Unit = ()

  protected def onCloseFailure(e: Throwable): Unit = throw e

  /** Blocks until all earlier tasks have completed and then prevents further tasks from being run.
    */
  final override def onFirstClose(): Unit = {
    import TraceContext.Implicits.Empty.*

    this.synchronizeWithReaders().discard[Boolean]
    try {
      onClosed()
    } catch {
      case NonFatal(e) => onCloseFailure(e)
    }
  }
}
