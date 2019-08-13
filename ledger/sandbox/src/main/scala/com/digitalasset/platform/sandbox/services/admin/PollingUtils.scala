// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import akka.actor.Scheduler
import akka.pattern.after
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object PollingUtils {

  private val logger: Logger = LoggerFactory.getLogger(PollingUtils.getClass)

  /**
    * Continuously polls the given service to check if the given item has been persisted.
    *
    * Despite the `go` inner function not being stack-safe per se, only one stack frame will be on
    * the stack at any given time since the "recursive" invocation happens on a different thread.
    *
    * The backoff waiting time are applied after the first poll returns without a result (i.e. the first call is not delayed).
    *
    * @param poll               The service, returning a collection of items
    * @param check              Returns true iff the service returned all items that are being waited for
    * @param description        A human readable description of the item that is being waited for, for logging purposes
    * @param minWait            The minimum waiting time - will not be enforced if less than `maxWait`
    *                           Does not make sense to set this lower than the OS scheduler threshold
    *                           Anyway always padded to 50 milliseconds
    * @param maxWait            The maximum waiting time - takes precedence over `minWait` and `backoffProgression`
    *                           Does not make sense to set this lower than the OS scheduler threshold
    *                           Anyway always padded to 50 milliseconds
    * @param backoffProgression How the following backoff time is computed as a function of the current one - `maxWait` takes precedence though
    * @return The number of attempts before the item was found wrapped in a [[Future]]
    */
  def pollUntilPersisted[T](poll: () => Future[T])(
      check: T => Boolean,
      description: String,
      minWait: FiniteDuration,
      maxWait: FiniteDuration,
      backoffProgression: FiniteDuration => FiniteDuration,
      scheduler: Scheduler): Future[Int] = {
    def go(attempt: Int, waitTime: FiniteDuration): Future[Int] = {
      logger.debug(s"Polling for '$description' being persisted (attempt #$attempt)...")
      poll()
        .flatMap {
          case persisted if check(persisted) => Future.successful(attempt)
          case _ =>
            logger.debug(s"'$description' not yet persisted, backing off for $waitTime...")
            after(waitTime, scheduler)(
              go(attempt + 1, backoffProgression(waitTime).min(maxWait).max(50.milliseconds)))(DE)
        }(DE)
        .recoverWith {
          case _ =>
            after(waitTime, scheduler)(
              go(attempt + 1, backoffProgression(waitTime).min(maxWait).max(50.milliseconds)))(DE)
        }(DE)
    }

    go(1, minWait.min(maxWait).max(50.milliseconds))
  }

}
