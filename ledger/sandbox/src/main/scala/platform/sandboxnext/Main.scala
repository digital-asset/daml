// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.resources.ResourceContext
import com.daml.cliopts.GlobalLogLevel
import com.daml.resources.ProgramResource

object Main {

  def main(args: Array[String]): Unit = {
    // This disables the automated shutdown by akka. we close everything in order,
    // so akka doesn't need to be clever and emit this warning when CTRL-C-ing the sandbox:
    // [CoordinatedShutdown(akka://sandbox)] Could not addJvmShutdownHook, due to: Shutdown in progress
    System.setProperty("akka.jvm-shutdown-hooks", "off")
    val config = Cli.parse(args).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set("Sandbox"))
    new ProgramResource(new Runner(config)).run(ResourceContext.apply)
  }

}
