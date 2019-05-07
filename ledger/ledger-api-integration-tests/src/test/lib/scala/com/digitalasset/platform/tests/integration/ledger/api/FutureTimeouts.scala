// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api
import akka.actor.ActorSystem
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.scalatest.concurrent.Waiters

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.control.NoStackTrace

trait FutureTimeouts extends Waiters {

  // TODO get rid of the default timeout, see issue: #464 and #548
  protected def timeout[T](f: Future[T], opName: String, duration: FiniteDuration = 500.seconds)(
      implicit system: ActorSystem): Future[T] = {
    val scaledDuration = scaled(duration)
    val promise: Promise[T] = Promise[T]()

    val cancellable = system.scheduler.scheduleOnce(
      scaledDuration, { () =>
        promise.failure(
          new TimeoutException(
            s"$opName timed out after $scaledDuration${if (duration != scaledDuration)
              s" (scaled from $duration)"}.") with NoStackTrace)
        ()
      }
    )(system.dispatcher)

    f.onComplete(_ => cancellable.cancel())(DirectExecutionContext)

    Future.firstCompletedOf(List(f, promise.future))(DirectExecutionContext)
  }
}
