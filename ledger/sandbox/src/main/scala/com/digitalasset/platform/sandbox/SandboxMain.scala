// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.concurrent.atomic.AtomicBoolean

import ch.qos.logback.classic.Level
import com.digitalasset.platform.sandbox.cli.Cli
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

object SandboxMain extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  Cli.parse(args).fold(sys.exit(1)) { config =>
    setGlobalLogLevel(config.logLevel)

    val server = new SandboxServer(config)

    val closed = new AtomicBoolean(false)

    def closeServer(): Unit = {
      if (closed.compareAndSet(false, true)) server.close()
    }

    server.failure.foreach { exception =>
      logger.error(
        s"Shutting down Sandbox application due to an initialization error:\n${exception.getMessage}")
      sys.exit(1)
    }

    try {
      Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
    } catch {
      case NonFatal(exception) =>
        logger.error("Shutting down Sandbox application due to an initialization error.", exception)
        closeServer()
        sys.exit(1)
    }
  }

  // Copied from language-support/scala/codegen/src/main/scala/com/digitalasset/codegen/Main.scala
  private def setGlobalLogLevel(verbosity: Level): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) match {
      case rootLogger: ch.qos.logback.classic.Logger =>
        rootLogger.setLevel(verbosity)
        rootLogger.info(s"Sandbox verbosity changed to $verbosity")
      case _ =>
        logger.warn(s"Sandbox verbosity cannot be set to requested $verbosity")
    }
  }

}
