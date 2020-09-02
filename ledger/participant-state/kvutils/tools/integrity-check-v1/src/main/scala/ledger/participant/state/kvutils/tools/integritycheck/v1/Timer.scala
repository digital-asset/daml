// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck.v1

import java.util.concurrent.TimeUnit

final class Timer {
  private var last = 0L
  private var total = 0L

  def time[T](act: => T): T = {
    val start = System.nanoTime()
    val result: T = act // force the thunk
    val end = System.nanoTime()
    last = end - start
    total += last
    result
  }

  def lastMs: Long = TimeUnit.NANOSECONDS.toMillis(last)

  def totalMs: Long = TimeUnit.NANOSECONDS.toMillis(total)
}
