// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.StartAndCloseable.StartAfterClose
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Trait to cleanly manage concurrent start and close operations
  *
  * This trait will help to manage start / close processes by delaying the
  * close operation until a start operation has succeeded and by
  * not executing a start operation during a shutdown
  */
trait StartAndCloseable[A] extends FlagCloseableAsync {

  private val startF = new SingleUseCell[Future[A]]()

  def isStarted: Boolean = startF.get.isDefined

  /** Start the process
    *
    * The method is idempotent.
    * It will not execute start twice.
    * It will also not start the process if close() has already been called. In that case, the returned future fails with
    * [[com.digitalasset.canton.lifecycle.StartAndCloseable.StartAfterClose]].
    * If close is called concurrently, it will delay the close until the start has succeeded.
    */
  final def start()(implicit ec: ExecutionContext, traceContext: TraceContext): Future[A] = {
    val outcomeF = internalPerformUnlessClosingF[A](functionFullName) {
      val promise = Promise[A]()
      val future = promise.future
      val previous = startF.putIfAbsent(future)
      previous match {
        case None =>
          promise.completeWith(startAsync())
          future
        case Some(previousStart) =>
          logger.debug("Not calling startAsync again")
          previousStart
      }
    }

    val res = outcomeF.onShutdown {
      // Return a failed future if we're not starting
      val alreadyClosing = Future.failed(StartAfterClose())
      val previous = startF.putIfAbsent(alreadyClosing)
      previous.getOrElse(alreadyClosing)
    }

    // This runs after `startF` has been updated because `performUnlessClosingF`
    // either evaluates the given function to get the future, which updates `startF`
    // or synchronously returns `None`, in which case the `outcomeF.onShutdown` sets `startF`.
    runStateChanged()

    res
  }

  protected def startAsync()(implicit tc: TraceContext): Future[A]

}

object StartAndCloseable {
  final case class StartAfterClose() extends RuntimeException
}
