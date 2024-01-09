// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.TimeServiceBackend
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.NoTracing

import java.time.Instant
import scala.concurrent.Future

/** Time service backend for testing only to enable the ledger api server to work with the Canton sim-clock or remote clock
  */
class CantonExternalClockBackend(clock: Clock, protected val loggerFactory: NamedLoggerFactory)
    extends TimeServiceBackend
    with NamedLogging
    with NoTracing {

  override def getCurrentTime: Instant = clock.now.toInstant

  override def setCurrentTime(currTime: Instant, newTime: Instant): Future[Boolean] = {
    logger.warn(
      s"CantonExternalClockBackend does not support setting time. Ignoring request to change time from $currTime to $newTime"
    )
    Future.successful(false)
  }
}
