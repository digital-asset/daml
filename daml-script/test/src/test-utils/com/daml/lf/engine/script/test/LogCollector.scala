// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase

import scala.collection.mutable.ArrayBuffer

// Custom log collector to test order of trace statements
object LogCollector {
  val events = new ArrayBuffer[ILoggingEvent]
  def clear(): Unit = events.clear
}

final class LogCollector extends AppenderBase[ILoggingEvent] {
  override def append(e: ILoggingEvent): Unit = {
    LogCollector.events += e
  }
}
