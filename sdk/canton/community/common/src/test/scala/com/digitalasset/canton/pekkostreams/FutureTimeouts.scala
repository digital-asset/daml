// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.DirectExecutionContext
import org.apache.pekko.actor.ActorSystem
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.Try
import scala.util.control.NoStackTrace

trait FutureTimeouts { self: AsyncWordSpec & BaseTest =>

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

    f.onComplete((_: Try[Any]) => cancellable.cancel())(DirectExecutionContext(noTracingLogger))

    recoverToSucceededIf[TimeoutException](
      Future.firstCompletedOf[Any](List[Future[Any]](f, promise.future))(
        DirectExecutionContext(noTracingLogger)
      )
    )
  }
}
