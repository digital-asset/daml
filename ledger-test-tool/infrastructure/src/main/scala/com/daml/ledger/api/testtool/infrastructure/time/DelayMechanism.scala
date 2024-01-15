// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.time

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.timer.Delayed

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

trait DelayMechanism {
  def delayBy(duration: Duration): Future[Unit]
}

class TimeDelayMechanism()(implicit ec: ExecutionContext) extends DelayMechanism {
  override def delayBy(duration: Duration): Future[Unit] = Delayed.by(duration)(())
}

class StaticTimeDelayMechanism(ledger: ParticipantTestContext)(implicit
    ec: ExecutionContext
) extends DelayMechanism {
  override def delayBy(duration: Duration): Future[Unit] =
    ledger
      .time()
      .flatMap { currentTime =>
        ledger.setTime(currentTime, currentTime.plusMillis(duration.toMillis))
      }
}
