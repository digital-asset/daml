// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.time

import java.time.{Clock, Instant}

import com.digitalasset.api.util.TimeProvider

class UtcWallClockTimeProvider extends TimeProvider {

  private val utcClock = Clock.systemUTC()

  override def getCurrentTime: Instant = utcClock.instant()

}
