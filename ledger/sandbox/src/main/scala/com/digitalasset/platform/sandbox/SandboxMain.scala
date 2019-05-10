// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.platform.sandbox.cli.Cli
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object SandboxMain extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sandboxServer: SandboxServer =
    Cli.parse(args).fold(sys.exit(1))(config => SandboxServer(config))

  private val closed = new AtomicBoolean(false)

  private def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) sandboxServer.close()
  }

  try {
    Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
    sandboxServer
  } catch {
    case NonFatal(t) => {
      logger.error("Shutting down Sandbox application because of initialization error", t)
      closeServer()
    }
  }

}
