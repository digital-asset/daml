// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.testing

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.backend.api.v1.SubmissionResult
import com.digitalasset.ledger.backend.api.v1.SubmissionResult.{Acknowledged, Error, Overloaded}
import com.digitalasset.platform.common.util.DirectExecutionContext

import scala.concurrent.Future

trait TimeServiceBackend extends TimeProvider {
  def getCurrentTime: Instant

  def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean]
}

object TimeServiceBackend {
  def simple(startTime: Instant): TimeServiceBackend =
    new SimpleTimeServiceBackend(startTime)

  def withObserver(
      timeProvider: TimeServiceBackend,
      onTimeChange: Instant => Future[SubmissionResult]): TimeServiceBackend =
    new ObservingTimeServiceBackend(timeProvider, onTimeChange)
}

private class SimpleTimeServiceBackend(startTime: Instant) extends TimeServiceBackend {
  private val timeRef = new AtomicReference[Instant](startTime)

  override def getCurrentTime: Instant = timeRef.get

  override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] = {
    val currentTime = timeRef.get
    val res = currentTime == expectedTime && timeRef.compareAndSet(currentTime, newTime)
    Future.successful(res)
  }
}

private class ObservingTimeServiceBackend(
    timeProvider: TimeServiceBackend,
    onTimeChange: Instant => Future[SubmissionResult]
) extends TimeServiceBackend {

  override def getCurrentTime: Instant = timeProvider.getCurrentTime

  override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] =
    timeProvider
      .setCurrentTime(expectedTime, newTime)
      .flatMap { success =>
        if (success)
          onTimeChange(newTime)
            .map {
              case Acknowledged => true
              case Overloaded => false
              case Error(t) =>
                sys.error(s"failed to react on time change ${t.getMessage}") // there is really nothing we can do here, but blow up
            }(DirectExecutionContext)
        else Future.successful(success)
      }(DirectExecutionContext)

}
