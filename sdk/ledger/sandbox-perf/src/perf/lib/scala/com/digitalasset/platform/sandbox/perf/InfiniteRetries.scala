// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import akka.actor.ActorSystem

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait InfiniteRetries {

  protected def retry[T](action: => Future[T], delay: FiniteDuration = 10.millis)(implicit
      system: ActorSystem
  ): Future[T] = {
    implicit val ec: ExecutionContext = system.dispatcher
    action.transformWith {
      case Success(v) =>
        Future.successful(v)
      case Failure(_) =>
        val p = Promise[T]()
        val r: Runnable = () =>
          retry[T](action, delay).onComplete {
            case Success(s) => p.success(s)
            case Failure(throwable) => p.failure(throwable)
          }
        system.scheduler.scheduleOnce(
          delay,
          r,
        )
        p.future
    }
  }
}
