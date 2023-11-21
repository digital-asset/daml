// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.logging.ErrorLoggingContext
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/** Promise that will log a message after logAfter if it has not been completed.
  */
class SupervisedPromise[T](
    description: String,
    futureSupervisor: FutureSupervisor,
    logAfter: Duration = 10.seconds,
    // TODO(i11704): lift to a higher level once known un-completed promises have been fixed
    logLevel: Level = Level.DEBUG,
)(implicit
    ecl: ErrorLoggingContext,
    ec: ExecutionContext,
) extends Promise[T] {
  private val promise: Promise[T] = Promise[T]()
  override def future: Future[T] = {
    futureSupervisor.supervised(description, logAfter, logLevel)(promise.future)
  }

  override def isCompleted: Boolean = promise.isCompleted
  override def tryComplete(result: Try[T]): Boolean = promise.tryComplete(result)
}
