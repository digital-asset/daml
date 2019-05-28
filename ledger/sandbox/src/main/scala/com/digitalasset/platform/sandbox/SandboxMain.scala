// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.platform.sandbox.cli.Cli
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object SandboxMain extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  Cli.parse(args).fold(sys.exit(1)) { config =>
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

}
