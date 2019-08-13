// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.concurrent.atomic.AtomicBoolean

import ch.qos.logback.classic.Level
import org.slf4j.Logger
import com.digitalasset.platform.sandbox.cli.Cli
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object SandboxMain extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  Cli.parse(args).fold(sys.exit(1)) { config =>
    setGlobalLogLevel(config.logLevel)

    val server = SandboxServer(config)

    val closed = new AtomicBoolean(false)

    def closeServer(): Unit = {
      if (closed.compareAndSet(false, true)) server.close()
    }

    try {
      Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
    } catch {
      case NonFatal(t) => {
        logger.error("Shutting down Sandbox application because of initialization error", t)
        closeServer()
      }
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
