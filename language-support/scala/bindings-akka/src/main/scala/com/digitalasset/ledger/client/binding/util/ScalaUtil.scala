// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.util

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}

object ScalaUtil {

  implicit class FutureOps[T](val future: Future[T]) extends LazyLogging {

    def timeout(
        name: String,
        failTimeout: FiniteDuration = 1.minute,
        warnTimeout: FiniteDuration = 30.seconds,
    )(implicit ec: ExecutionContext, scheduler: ScheduledExecutorService): Future[T] = {

      val promise = Promise[T]()

      @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
      val warningTask = schedule(warnTimeout) {
        logger.warn("Function {} takes more than {}", name, warnTimeout: Any)
      }

      val errorTask = schedule(failTimeout) {
        val error = new TimeoutException(s"Function call $name took more than $failTimeout")
        promise.tryFailure(error)
        ()
      }

      future.onComplete { outcome =>
        warningTask.cancel(false)
        errorTask.cancel(false)
        promise.tryComplete(outcome)
      }

      promise.future
    }

    private def schedule(
        timeout: FiniteDuration
    )(f: => Unit)(implicit scheduler: ScheduledExecutorService): ScheduledFuture[_] = {

      val runnable = new Runnable {
        override def run(): Unit = f
      }

      scheduler.schedule(runnable, timeout.toMillis, TimeUnit.MILLISECONDS)
    }

    def timeoutWithDefaultWarn(name: String, failTimeout: FiniteDuration)(implicit
        ec: ExecutionContext,
        scheduler: ScheduledExecutorService,
    ): Future[T] = timeout(name, failTimeout, 10.seconds)

  }

}
