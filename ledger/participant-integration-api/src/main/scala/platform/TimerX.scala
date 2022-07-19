// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

object TimerX {

  object Original {
    val verboseEnriching = new TimerX
    val apiSerialization = new TimerX
    val dbDeserialization = new TimerX
  }

  object InterfaceProjection {
    val computeInterfaceView = new TimerX
    val apiSerialization = new TimerX
  }
}

class TimerX {
  private var nanos: Long = 0L

  def measure[T](t: => T): T = {
    val start = System.nanoTime()
    val result = t
    val end = System.nanoTime()
    synchronized(nanos += (end - start))
    result
  }

  // (millis, nanos)
  def reset(): (Long, Long) = synchronized {
    val sumMillis = nanos / 1000000
    val sumNanos = nanos - ((nanos / 1000000) * 1000000)
    nanos = 0L
    sumMillis -> sumNanos
  }
}
