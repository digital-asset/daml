// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DiscardOps, config}

import java.util.{Timer, TimerTask}
import scala.concurrent.Promise
import scala.util.Try
import scala.util.control.NonFatal

trait CancellableTimeoutSupport {
  def scheduleOnce[T](
      duration: config.NonNegativeFiniteDuration,
      promise: Promise[T],
      onTimeout: => Try[T],
  )(implicit traceContext: TraceContext): AutoCloseable
}

object CancellableTimeoutSupport {
  def owner(
      timerThreadName: String,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[CancellableTimeoutSupport] =
    ResourceOwner
      .forTimer(() => new Timer(timerThreadName, true))
      .map(new CancellableTimeoutSupportImpl(_, loggerFactory))
}

private[tracking] class CancellableTimeoutSupportImpl(
    timer: Timer,
    val loggerFactory: NamedLoggerFactory,
) extends CancellableTimeoutSupport
    with NamedLogging {
  override def scheduleOnce[T](
      duration: config.NonNegativeFiniteDuration,
      promise: Promise[T],
      onTimeout: => Try[T],
  )(implicit traceContext: TraceContext): AutoCloseable = {
    val timerTask = new TimerTask {
      override def run(): Unit =
        try {
          promise.tryComplete(onTimeout).discard
        } catch {
          case NonFatal(e) =>
            val exceptionMessage =
              "Exception thrown in complete. Resources might have not been appropriately cleaned"
            logger.error(exceptionMessage, e)
        }
    }
    timer.schedule(timerTask, duration.underlying.toMillis)
    () => timerTask.cancel().discard
  }
}
