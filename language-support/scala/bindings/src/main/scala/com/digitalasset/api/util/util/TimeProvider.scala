// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.api.util

import java.time.{Clock, Instant}

import com.daml.api.util.TimeProvider.MappedTimeProvider

trait TimeProvider { self =>

  def getCurrentTime: Instant

  def map(transform: Instant => Instant): TimeProvider = MappedTimeProvider(this, transform)
}

object TimeProvider {
  final case class MappedTimeProvider(timeProvider: TimeProvider, transform: Instant => Instant)
      extends TimeProvider {
    override def getCurrentTime: Instant = transform(timeProvider.getCurrentTime)
  }

  final case class Constant(getCurrentTime: Instant) extends TimeProvider

  case object UTC extends TimeProvider {

    private val utcClock = Clock.systemUTC()

    override def getCurrentTime: Instant = utcClock.instant()
  }

}
