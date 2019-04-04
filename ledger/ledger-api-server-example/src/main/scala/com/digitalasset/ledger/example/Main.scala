// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.example

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.ledger.example.Application.Server
import com.digitalasset.platform.sandbox.cli.Cli
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.While"))
object Main extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val server: Server = {
    Cli
      .parse(args.filterNot(s => s.startsWith("--config-file")))
      .fold(sys.exit(1))(config => Application(config))
  }

  private val closed = new AtomicBoolean(false)
  private def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) server.close()
  }

  try {
    Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
    server.start()
  } catch {
    case NonFatal(t) => {
      logger.error("Shutting down Ledger API server because of an initialization error", t)
      closeServer()
    }
  }

}
