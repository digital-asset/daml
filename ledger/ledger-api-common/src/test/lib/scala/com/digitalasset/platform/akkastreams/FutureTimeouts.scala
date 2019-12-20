// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams
import akka.actor.ActorSystem
import com.digitalasset.dec.DirectExecutionContext
import org.scalatest.{Assertion, AsyncWordSpec}

import scala.concurrent.{Future, Promise, TimeoutException}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NoStackTrace

trait FutureTimeouts { self: AsyncWordSpec =>

  protected def system: ActorSystem

  protected def expectTimeout(f: Future[Any], duration: FiniteDuration): Future[Assertion] = {
    val promise: Promise[Any] = Promise[Any]()

    val cancellable = system.scheduler.scheduleOnce(duration, { () =>
      promise.failure(
        new TimeoutException(s"Future timed out after $duration as expected.") with NoStackTrace)
      ()
    })(system.dispatcher)

    f.onComplete((_: Try[Any]) => cancellable.cancel())(DirectExecutionContext)

    recoverToSucceededIf[TimeoutException](
      Future.firstCompletedOf[Any](List[Future[Any]](f, promise.future))(DirectExecutionContext))
  }
}
