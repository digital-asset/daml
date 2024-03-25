// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.boolex.{EvaluationException, EventEvaluatorBase}
import ch.qos.logback.core.filter.EvaluatorFilter
import ch.qos.logback.core.spi.FilterReply

/** Filters out any events with log level strictly lower than "LOG_LEVEL_CANTON" and
  * logger name starting with "com.daml" or "com.digitalasset".
  * The property "LOG_LEVEL_CANTON" is first read from java system properties, then from the shell environment and then the default of INFO is taken.
  * (This is consistent with the standard logback behavior.)
  */
class CantonFilterEvaluator extends EvaluatorFilter[ILoggingEvent] {

  private val mine = new EventEvaluatorBase[ILoggingEvent]() {
    @throws[NullPointerException]
    @throws[EvaluationException]
    override def evaluate(event: ILoggingEvent): Boolean = {
      val cantonLevelStr = Option(System.getProperty("LOG_LEVEL_CANTON"))
        .orElse(Option(System.getenv("LOG_LEVEL_CANTON")))
        .getOrElse("INFO")
      val cantonLevel = Level.toLevel(cantonLevelStr)
      val logger = event.getLoggerName

      (logger.startsWith("com.digitalasset") || logger.startsWith("com.daml")) &&
      event.getLevel.levelInt < cantonLevel.toInteger
    }
  }

  this.setEvaluator(mine)
  this.setOnMatch(FilterReply.DENY)
  this.setOnMismatch(FilterReply.NEUTRAL)

  override def start(): Unit = {
    if (!mine.isStarted) mine.start()
    super.start()
  }

}
