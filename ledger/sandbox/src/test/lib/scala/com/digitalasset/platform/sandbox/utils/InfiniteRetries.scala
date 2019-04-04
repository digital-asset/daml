// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.utils

import akka.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait InfiniteRetries {

  protected def retry[T](action: => Future[T], delay: FiniteDuration = 10.millis)(
      implicit system: ActorSystem): Future[T] = {
    implicit val ec: ExecutionContext = system.dispatcher
    action.transformWith {
      case Success(v) =>
        Future.successful(v)
      case Failure(t) =>
        val p = Promise[T]()
        system.scheduler.scheduleOnce(
          delay,
          () =>
            retry[T](action, delay).onComplete {
              case Success(s) => p.success(s)
              case Failure(throwable) => p.failure(throwable)
          }
        )
        p.future
    }
  }
}
