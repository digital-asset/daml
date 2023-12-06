// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.TimeServiceBackend
import com.digitalasset.canton.time.{Clock, TestingTimeService}
import com.digitalasset.canton.tracing.NoTracing

import java.time.Instant
import scala.concurrent.Future

/** Optional time services backend for demos and testing only to enable an "advanceable clock" based on the environment's
  * sim-clocks. Canton only supports advancing time, not going backwards in time.
  */
class CantonTimeServiceBackend(
    clock: Clock,
    testingTimeService: TestingTimeService,
    protected val loggerFactory: NamedLoggerFactory,
) extends TimeServiceBackend
    with NamedLogging
    with NoTracing {

  override def getCurrentTime: Instant = clock.now.toInstant

  override def setCurrentTime(currTimeInst: Instant, newTimeInst: Instant): Future[Boolean] = {
    val advanced = for {
      currTime <- CantonTimestamp.fromInstant(currTimeInst)
      newTime <- CantonTimestamp.fromInstant(newTimeInst)
      res <- testingTimeService.advanceTime(currTime, newTime)
    } yield res
    val error = advanced.fold(err => { logger.warn(err); false }, _ => true)
    Future.successful(error)
  }
}
