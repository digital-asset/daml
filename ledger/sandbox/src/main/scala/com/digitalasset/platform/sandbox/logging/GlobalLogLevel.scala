// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import ch.qos.logback.classic.Level
import org.slf4j.{Logger, LoggerFactory}

object GlobalLogLevel {
  def set(level: Level): Unit = {
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
    LoggerFactory.getILoggerFactory match {
      case loggerContext: ch.qos.logback.classic.LoggerContext =>
        rootLogger.info(s"Sandbox verbosity changed to $level")
        loggerContext.getLoggerList.forEach(_.setLevel(level))
      case _ =>
        rootLogger.warn(s"Sandbox verbosity cannot be set to requested $level")
    }
  }
}
