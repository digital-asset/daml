// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.akkastreams
import akka.actor.ActorSystem
import com.daml.dec.DirectExecutionContext
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise, TimeoutException}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NoStackTrace

trait FutureTimeouts { self: AsyncWordSpec =>

  protected def system: ActorSystem

  protected def expectTimeout(f: Future[Any], duration: FiniteDuration): Future[Assertion] = {
    val promise: Promise[Any] = Promise[Any]()

    val runnable: Runnable = () =>
      promise.failure(
        new TimeoutException(s"Future timed out after $duration as expected.") with NoStackTrace
      )
    val cancellable = system.scheduler.scheduleOnce(
      duration,
      runnable,
    )(system.dispatcher)

    f.onComplete((_: Try[Any]) => cancellable.cancel())(DirectExecutionContext)

    recoverToSucceededIf[TimeoutException](
      Future.firstCompletedOf[Any](List[Future[Any]](f, promise.future))(DirectExecutionContext)
    )
  }
}
