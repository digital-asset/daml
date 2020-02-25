// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.dec.DirectExecutionContext

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
      onTimeChange: Instant => Future[Unit]): TimeServiceBackend =
    new ObservingTimeServiceBackend(timeProvider, onTimeChange)

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
      onTimeChange: Instant => Future[Unit]
  ) extends TimeServiceBackend {
    override def getCurrentTime: Instant = timeProvider.getCurrentTime

    override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] =
      timeProvider
        .setCurrentTime(expectedTime, newTime)
        .flatMap { success =>
          if (success)
            onTimeChange(expectedTime).map(_ => true)(DirectExecutionContext)
          else Future.successful(false)
        }(DirectExecutionContext)
  }
}
