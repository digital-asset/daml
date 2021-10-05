// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.cliopts.GlobalLogLevel
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.resources.ProgramResource

object SandboxMain {

  private val logger = ContextualizedLogger.get(this.getClass)

  def main(args: Array[String]): Unit =
    new ProgramResource({
      val config: SandboxConfig = Cli.parse(args).getOrElse(sys.exit(1))
      config.logLevel.foreach(GlobalLogLevel.set("Sandbox"))

      // Point users to non-deprecated ledgers
      config.jdbcUrl.foreach(_ =>
        newLoggingContext { implicit loggingContext =>
          logger.info(
            "Sandbox classic with persistence is deprecated. Use the Daml Driver for PostgreSQL if you need persistence."
          )
        }
      )
      SandboxServer.owner(Name, config)
    }).run(ResourceContext.apply)

}
