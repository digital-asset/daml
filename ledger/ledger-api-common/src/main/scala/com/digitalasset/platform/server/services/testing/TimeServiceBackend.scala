// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.testing

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.platform.common.util.DirectExecutionContext

import scala.concurrent.Future

trait TimeServiceBackend extends TimeProvider {
  def getCurrentTime: Instant

  def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean]

  def allowSettingTimeBackwards: Boolean
}

object TimeServiceBackend {
  def simple(startTime: Instant, allowSettingTimeBackwards: Boolean): TimeServiceBackend =
    new SimpleTimeServiceBackend(startTime, allowSettingTimeBackwards)

  def withObserver(
      timeProvider: TimeServiceBackend,
      onTimeChange: Instant => Future[Unit]): TimeServiceBackend =
    new ObservingTimeServiceBackend(timeProvider, onTimeChange)
}

private class SimpleTimeServiceBackend(startTime: Instant, val allowSettingTimeBackwards: Boolean)
    extends TimeServiceBackend {
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

  override def allowSettingTimeBackwards: Boolean = timeProvider.allowSettingTimeBackwards

}
