// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import ch.qos.logback.classic.Level
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.resources.ProgramResource
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

object SandboxMain extends App {
  private implicit val executionContext: ExecutionContext = DirectExecutionContext

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  Cli.parse(args).fold(sys.exit(1)) { config =>
    setGlobalLogLevel(config.logLevel)
    new ProgramResource(SandboxServer.owner(config)).run()
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
