// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.cliopts

import org.slf4j.{Logger, LoggerFactory}
import ch.qos.logback.classic.{Level => LogLevel}

object GlobalLogLevel {
  def set(serviceName: String)(level: LogLevel): Unit = {
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
    LoggerFactory.getILoggerFactory match {
      case loggerContext: ch.qos.logback.classic.LoggerContext =>
        rootLogger.info(s"${serviceName} verbosity changed to $level")
        loggerContext.getLoggerList.forEach(_.setLevel(level))
      case _ =>
        rootLogger.warn(s"${serviceName} verbosity cannot be set to requested $level")
    }
  }
}
