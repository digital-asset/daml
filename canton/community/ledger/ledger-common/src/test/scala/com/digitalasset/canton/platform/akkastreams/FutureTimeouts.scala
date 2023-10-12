// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.akkastreams

import akka.actor.ActorSystem
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
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

    f.onComplete((_: Try[Any]) => cancellable.cancel())(ExecutionContext.parasitic)

    recoverToSucceededIf[TimeoutException](
      Future.firstCompletedOf[Any](List[Future[Any]](f, promise.future))(ExecutionContext.parasitic)
    )
  }
}
