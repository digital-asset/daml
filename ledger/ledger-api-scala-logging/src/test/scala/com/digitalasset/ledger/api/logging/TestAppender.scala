// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase

import scala.collection.mutable.ArrayBuffer

class TestAppender extends AppenderBase[ILoggingEvent] {
  import TestAppender._
  override def append(event: ILoggingEvent): Unit = TestAppender.synchronized[Unit] {
    if (enabled) loggingEvents += event
  }
}

object TestAppender {
  @volatile private var enabled = false

  def isEnabled: Boolean = enabled

  def enable(): Unit = enabled = true

  def disable(): Unit = this.synchronized {
    loggingEvents.clear()
    enabled = false
  }

  private val loggingEvents = new ArrayBuffer[ILoggingEvent]()

  def flushLoggingEvents: Vector[ILoggingEvent] = this.synchronized {
    val retVal = loggingEvents.toVector
    loggingEvents.clear()
    retVal
  }
  def clearLoggingEvents(): Unit = this.synchronized {
    loggingEvents.clear()
  }

}
