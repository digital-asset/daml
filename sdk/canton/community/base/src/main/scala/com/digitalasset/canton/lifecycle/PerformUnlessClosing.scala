// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.EitherT
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{Checked, CheckedT}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
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

  /** How often to poll to check that all tasks have completed. */
  protected def maxSleepMillis: Long = 500

  /** Performs the task given by `f` unless a shutdown has been initiated. The shutdown will only
    * begin after `f` completes, but other tasks may execute concurrently with `f`, if started using
    * this function, or one of the other variants ([[performUnlessClosingF]] and
    * [[performUnlessClosingEitherT]]). The tasks are assumed to take less than [[closingTimeout]]
    * to complete.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock.
    *
    * @param f
    *   The task to perform
    * @return
    *   [[scala.None$]] if a shutdown has been initiated. Otherwise the result of the task.
    */
  def performUnlessClosing[A](
      name: String
  )(f: => A)(implicit traceContext: TraceContext): UnlessShutdown[A] =
    this.addReader(name) match {
      case UnlessShutdown.Outcome(handle) =>
        try {
          UnlessShutdown.Outcome(f)
        } finally {
          this.removeReader(handle)
        }
      case AbortedDueToShutdown =>
        logger.debug(s"Won't schedule the task '$name' as this object is closing")
        UnlessShutdown.AbortedDueToShutdown
    }

  /** Performs the Future given by `f` unless a shutdown has been initiated. The future is lazy and
    * not evaluated during shutdown. The shutdown will only begin after `f` completes, but other
    * tasks may execute concurrently with `f`, if started using this function, or one of the other
    * variants ([[performUnlessClosing]] and [[performUnlessClosingEitherT]]). The tasks are assumed
    * to take less than [[closingTimeout]] to complete.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock.
    *
    * @param f
    *   The task to perform
    * @return
    *   The future completes with
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if a shutdown has
    *   been initiated. Otherwise the result of the task wrapped in
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]].
    */
  def performUnlessClosingF[A](name: String)(
      f: => Future[A]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    performUnlessClosingUSF(name)(FutureUnlessShutdown.outcomeF(f))

  def performUnlessClosingUSF[A](name: String)(
      f: => FutureUnlessShutdown[A]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    this.addReader(name) match {
      case UnlessShutdown.Outcome(handle) =>
        Try(f).fold(FutureUnlessShutdown.failed[A], x => x).thereafter { _ =>
          this.removeReader(handle)
        }
      case AbortedDueToShutdown =>
        logger.debug(s"Won't schedule the future '$name' as this object is closing")
        FutureUnlessShutdown.abortedDueToShutdown
    }

  /** Use this method if closing/shutdown of the object should wait for asynchronous computation to
    * finish too.
    *
    * @param f
    *   closing of this object will wait for all such spawned Futures to finish
    * @param asyncResultToWaitForF
    *   closing of this object will wait also wait for all such asynchronous Futures to finish too
    * @return
    *   the future spawned by f
    */
  def performUnlessClosingUSFAsync[A](name: String)(
      f: => FutureUnlessShutdown[A]
  )(
      asyncResultToWaitForF: A => FutureUnlessShutdown[?]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    this.addReader(name) match {
      case UnlessShutdown.Outcome(handle) =>
        val fut = Try(f).fold(FutureUnlessShutdown.failed[A], identity)
        fut
          .flatMap(asyncResultToWaitForF)
          .thereafter(_ => this.removeReader(handle))
          // TODO(#16601) Do not discard a future here
          .discard
        fut
      case AbortedDueToShutdown =>
        logger.debug(s"Won't schedule the future '$name' as this object is closing")
        FutureUnlessShutdown.abortedDueToShutdown
    }

  /** Performs the EitherT[Future] given by `etf` unless a shutdown has been initiated, in which
    * case the provided error is returned instead. Both `etf` and the error are lazy; `etf` is only
    * evaluated if there is no shutdown, the error only if we're shutting down. The shutdown will
    * only begin after `etf` completes, but other tasks may execute concurrently with `etf`, if
    * started using this function, or one of the other variants ([[performUnlessClosing]] and
    * [[performUnlessClosingF]]). The tasks are assumed to take less than [[closingTimeout]] to
    * complete.
    *
    * DO NOT CALL `this.close` as part of `etf`, because it will result in a deadlock.
    *
    * @param etf
    *   The task to perform
    */
  def performUnlessClosingEitherT[E, R](name: String, onClosing: => E)(
      etf: => EitherT[Future, E, R]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, E, R] =
    EitherT(performUnlessClosingF(name)(etf.value).unwrap.map(_.onShutdown(Left(onClosing))))

  def performUnlessClosingEitherU[E, R](name: String)(
      etf: => EitherT[Future, E, R]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, E, R] =
    EitherT(performUnlessClosingF(name)(etf.value))

  def performUnlessClosingEitherUSF[E, R](name: String)(
      etf: => EitherT[FutureUnlessShutdown, E, R]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, E, R] =
    EitherT(performUnlessClosingUSF(name)(etf.value))

  /** Use this method if closing/shutdown of the object should wait for asynchronous computation to
    * finish too.
    *
    * @param etf
    *   closing of this object will wait for all such spawned Futures to finish
    * @param asyncResultToWaitForF
    *   closing of this object will wait also wait for all such asynchronous Futures to finish too
    * @return
    *   the future spawned by etf
    */
  def performUnlessClosingEitherUSFAsync[E, R](name: String)(
      etf: => EitherT[FutureUnlessShutdown, E, R]
  )(
      asyncResultToWaitForF: R => FutureUnlessShutdown[?]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, E, R] =
    EitherT(
      performUnlessClosingUSFAsync(name)(etf.value)(
        _.map(asyncResultToWaitForF).getOrElse(FutureUnlessShutdown.unit)
      )
    )

  def performUnlessClosingCheckedT[A, N, R](name: String, onClosing: => Checked[A, N, R])(
      etf: => CheckedT[Future, A, N, R]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): CheckedT[Future, A, N, R] =
    CheckedT(performUnlessClosingF(name)(etf.value).unwrap.map(_.onShutdown(onClosing)))

  def performUnlessClosingCheckedUST[A, N, R](name: String, onClosing: => Checked[A, N, R])(
      etf: => CheckedT[FutureUnlessShutdown, A, N, R]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): CheckedT[FutureUnlessShutdown, A, N, R] =
    CheckedT(
      FutureUnlessShutdown.outcomeF(
        performUnlessClosingUSF(name)(etf.value).unwrap.map(_.onShutdown(onClosing))
      )
    )

  protected def onClosed(): Unit = ()

  protected def onCloseFailure(e: Throwable): Unit = throw e

  /** Blocks until all earlier tasks have completed and then prevents further tasks from being run.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Var"))
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
