// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.clock

import java.time.{Clock, Duration, Instant, ZoneId}

case class AdjustableClock(baseClock: Clock, var offset: Duration) extends Clock {
  def fastForward(by: Duration): Unit =
    offset = offset.plus(by)

  def rewind(by: Duration): Unit =
    offset = offset.minus(by)

  def set(to: Instant): Unit =
    offset = Duration.between(baseClock.instant(), to)

  override def getZone: ZoneId = baseClock.getZone

  override def withZone(zone: ZoneId): Clock =
    if (zone == baseClock.getZone) this
    else AdjustableClock(baseClock.withZone(zone), offset)

  override def millis: Long = Math.addExact(baseClock.millis, offset.toMillis)

  override def instant: Instant = baseClock.instant.plus(offset)

  override def equals(obj: Any): Boolean =
    if (!obj.isInstanceOf[AdjustableClock]) false
    else {
      val other = obj.asInstanceOf[AdjustableClock]
      baseClock == other.baseClock && offset == other.offset
    }

  override def hashCode: Int = baseClock.hashCode ^ offset.hashCode
}
