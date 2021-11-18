// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import java.util.concurrent.Executors
import com.daml.ledger.resources.ResourceContext
import com.daml.cliopts.GlobalLogLevel
import com.daml.platform.sandbox.config.{PostgresStartupMode, SandboxConfig}
import com.daml.platform.sandbox.SandboxServer
import com.daml.resources.ProgramResource

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main {

  def main(args: Array[String]): Unit = {
    val config = new Cli().parse(args).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set("Daml-on-sql"))
    run(config)
  }

  private[sql] def run(config: SandboxConfig): Unit =
    config.sqlStartMode.foreach {
      case PostgresStartupMode.MigrateOnly =>
        val cachedThreadPool = Executors.newCachedThreadPool()
        implicit val executionContext: ExecutionContext =
          ExecutionContext.fromExecutorService(cachedThreadPool)
        implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

        SandboxServer.migrateOnly(config).onComplete {
          case Success(_) =>
            cachedThreadPool.shutdown()
            sys.exit(0)
          case Failure(exception) =>
            System.err.println(exception.getMessage)
            cachedThreadPool.shutdown()
            sys.exit(1)
        }

      case _ =>
        new ProgramResource(SandboxServer.owner(Name, config)).run(ResourceContext.apply)
    }
}
