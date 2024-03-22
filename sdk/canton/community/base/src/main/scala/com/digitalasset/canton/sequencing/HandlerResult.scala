// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}

import scala.concurrent.{ExecutionContext, Future}

object HandlerResult {

  /** Denotes that the synchronous processing stage for an event has completed
    * and there is no asynchronous processing for this stage.
    */
  val done: HandlerResult = FutureUnlessShutdown.pure(AsyncResult.immediate)

  /** The given [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] shall be run synchronously, i.e.,
    * later stages of processing the request will not start until this future has completed
    * with a [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]].
    * Later requests will also only be processed after this future has completed
    * with an [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]].
    * If the future fails with an exception or
    * returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]],
    * the sequencer client will close the subscription.
    */
  def synchronous(future: FutureUnlessShutdown[Unit])(implicit
      ec: ExecutionContext
  ): HandlerResult =
    future.map(_ => AsyncResult.immediate)

  /** Embeds an evaluated [[com.digitalasset.canton.lifecycle.UnlessShutdown]]
    * into a [[synchronous]] [[HandlerResult]].
    */
  def unlessShutdown(x: UnlessShutdown[Unit]): HandlerResult =
    FutureUnlessShutdown.lift(x.map(_ => AsyncResult.immediate))

  /** Shorthand for `synchronous(FutureUnlessShutdown.outcomeF(future))` */
  def fromFuture(future: Future[Unit])(implicit ec: ExecutionContext): HandlerResult =
    synchronous(FutureUnlessShutdown.outcomeF(future))

  /** The given [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]]
    * is an asynchronous processing part for the event.
    * It can run in parallel with any of the following:
    * * Earlier events' asynchronous processing
    * * Later events' synchronous and asynchronous processing
    * * Later stages of synchronous processing for the same event
    * The event will be marked as clean only after the future has completed successfully
    * with [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]].
    * If the future fails with an exception or
    * returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]],
    * the sequencer client will eventually close the subscription.
    */
  def asynchronous(future: FutureUnlessShutdown[Unit]): HandlerResult =
    FutureUnlessShutdown.pure(AsyncResult(future))
}
