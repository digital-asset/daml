// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.http.util.Logging.InstanceUUID
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.MonadError
import scalaz.Scalaz._

object Concurrent {

  private[this] val logger = ContextualizedLogger.get(getClass)

  /** Equal to a Semaphore with max count equal to one. */
  final case class Mutex() extends java.util.concurrent.Semaphore(1)

  def executeIfAvailable[F[_], A](mutex: Mutex)(fa: => F[A])(
      ifUnavailable: => F[A]
  )(implicit lc: LoggingContextOf[InstanceUUID], me: MonadError[F, Throwable]): F[A] = {
    logger.trace("Trying to acquire mutex")
    if (mutex.tryAcquire())
      try me.pure(logger.trace("Acquired mutex")) *> fa.attempt.flatMap { res =>
        // regardless of whether this was a success or failure
        // we always want to release the mutex here.
        mutex.release()
        logger.trace("Released mutex after task finished")
        res.fold(me.raiseError, me.pure(_))
      } catch {
        case t: Throwable =>
          mutex.release()
          logger.trace("Released mutex after catching exception")
          me.raiseError(t)
      }
    else ifUnavailable
  }
}
