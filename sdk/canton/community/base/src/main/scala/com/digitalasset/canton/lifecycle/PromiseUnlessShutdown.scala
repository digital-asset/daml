// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.logging.ErrorLoggingContext
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/** A wrapper for Promise that provides supervision of uncompleted promise's futures and aborting a promise due to shutdown */
class PromiseUnlessShutdown[A](
    description: String,
    futureSupervisor: FutureSupervisor,
    logAfter: Duration = 10.seconds,
    logLevel: Level = Level.DEBUG,
)(implicit
    ecl: ErrorLoggingContext,
    ec: ExecutionContext,
) extends Promise[UnlessShutdown[A]]
    with RunOnShutdown {

  private val promise: SupervisedPromise[UnlessShutdown[A]] =
    new SupervisedPromise[UnlessShutdown[A]](description, futureSupervisor, logAfter, logLevel)

  override def future: Future[UnlessShutdown[A]] = promise.future

  override def isCompleted: Boolean = promise.isCompleted

  override def tryComplete(result: Try[UnlessShutdown[A]]): Boolean = promise.tryComplete(result)

  def completeWith(other: FutureUnlessShutdown[A]): PromiseUnlessShutdown.this.type =
    super.completeWith(other.unwrap)

  def futureUS: FutureUnlessShutdown[A] = FutureUnlessShutdown(future)

  /** Complete the promise with an outcome value.
    * If the promise has already been completed with an outcome, the new outcome will be ignored.
    */
  def outcome(value: A): Unit =
    super.trySuccess(UnlessShutdown.Outcome(value)).discard

  /** Complete the promise with a shutdown */
  def shutdown(): Unit = promise.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard

  override def name: String = description
  override def done: Boolean = isCompleted
  override def run(): Unit = shutdown()
}
