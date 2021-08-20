// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.http.util.Logging.InstanceUUID
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.MonadError
import scalaz.Scalaz._

import java.util.concurrent.TimeUnit

object Concurrent {

  private[this] val logger = ContextualizedLogger.get(getClass)

  /** Equal to a Semaphore with max count equal to one. */
  final case class Mutex() extends java.util.concurrent.Semaphore(1, true)

}
