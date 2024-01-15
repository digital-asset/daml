// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.{Timer, TimerTask}
import scala.concurrent.{Future, Promise}
import scala.util.Try

class TimerResourceOwner[Context: HasExecutionContext](
    acquireTimer: () => Timer,
    waitForRunningTasks: Boolean,
) extends AbstractResourceOwner[Context, Timer] {
  override def acquire()(implicit context: Context): Resource[Context, Timer] =
    ReleasableResource(Future(acquireTimer())) { timer =>
      val timerCancelledPromise = Promise[Unit]()
      Future(
        // We are cancel()-ing the timer in a scheduled task to make sure no scheduled tasks are running
        // as the Timer Resource is released. See Timer.cancel() method's Java Documentation for more information.
        timer.schedule(
          new TimerTask {
            override def run(): Unit =
              timerCancelledPromise.complete(
                Try(timer.cancel())
              )
          },
          0L,
        )
      )
        // if timer.schedule fails, we do not want to wait for the timerCancelledPromise
        .flatMap(_ =>
          if (waitForRunningTasks) {
            // waiting for cancellation
            timerCancelledPromise.future
          } else {
            // not waiting for cancellation, finish Resource releasing right away
            Future.unit
          }
        )
    }
}
