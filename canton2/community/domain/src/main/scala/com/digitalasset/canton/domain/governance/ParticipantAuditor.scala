// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.governance

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import org.slf4j.helpers.NOPLogger.NOP_LOGGER

class ParticipantAuditor(val loggerFactory: NamedLoggerFactory) extends NamedLogging {}

object ParticipantAuditor {

  val noop: TracedLogger = TracedLogger(NOP_LOGGER)

  def factory(loggerFactory: NamedLoggerFactory, enable: Boolean): TracedLogger =
    if (enable) {
      val ret = new ParticipantAuditor(loggerFactory).logger
      ret
    } else {
      NamedLogging.noopLogger
    }

}
