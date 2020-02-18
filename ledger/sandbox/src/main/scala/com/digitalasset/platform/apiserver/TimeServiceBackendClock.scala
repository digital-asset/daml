// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

class TimeServiceBackendClock(
    backend: TimeServiceBackend,
    override val getZone: ZoneId = ZoneOffset.UTC,
) extends Clock {
  override def instant(): Instant =
    backend.getCurrentTime

  override def withZone(zone: ZoneId): Clock =
    new TimeServiceBackendClock(backend, zone)
}
