// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import java.time.Duration

trait HasUptime {

  /** The clock used to measure up-time */
  protected def clock: Clock

  private val startupTime = clock.now

  protected def uptime(): Duration = Duration.between(startupTime.toInstant, clock.now.toInstant)
}
