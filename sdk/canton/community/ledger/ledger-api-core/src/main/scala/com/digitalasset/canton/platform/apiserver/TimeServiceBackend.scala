// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.ledger.api.util.TimeProvider

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

trait TimeServiceBackend extends TimeProvider {
  def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean]
}

object TimeServiceBackend {
  def simple(startTime: Instant): TimeServiceBackend =
    new SimpleTimeServiceBackend(startTime)

  private final class SimpleTimeServiceBackend(startTime: Instant) extends TimeServiceBackend {
    private val timeRef = new AtomicReference[Instant](startTime)

    override def getCurrentTime: Instant = timeRef.get

    override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] = {
      val currentTime = timeRef.get
      val res = currentTime == expectedTime && timeRef.compareAndSet(currentTime, newTime)
      Future.successful(res)
    }
  }
}
