// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.log

import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.spi.FilterReply

/** Logs at specific level and above, excluded logger logs everything.
  * If you don't have exclusions, use regular ThresholdFilter. Without provided excludeLogger this
  * implementation logs every event with no filtering at all.
  */
class LogbackThresholdFilterWithExclusion extends ThresholdFilter {
  @volatile private var excludeLogger: String = _

  def setExcludeLogger(a: String): Unit = this.excludeLogger = a

  def getExcludeLogger: String = this.excludeLogger

  override def decide(event: ILoggingEvent): FilterReply = {
    if (!this.isStarted) FilterReply.NEUTRAL
    else if (event.getLoggerName.startsWith(excludeLogger)) FilterReply.NEUTRAL
    else super.decide(event)
  }

  override def start(): Unit = {
    if (this.excludeLogger != null) {
      super.start()
    }
  }
}
