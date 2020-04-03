// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import com.digitalasset.platform.sandbox.GlobalLogLevel
import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    // This disables the automated shutdown by akka. we close everything in order,
    // so akka doesn't need to be clever and emit this warning when CTRL-C-ing the sandbox:
    // [CoordinatedShutdown(akka://sandbox)] Could not addJvmShutdownHook, due to: Shutdown in progress
    System.setProperty("akka.jvm-shutdown-hooks", "off")
    val config = Cli.parse(args, SandboxConfig.nextDefault).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set)
    new ProgramResource(new Runner(config)).run()
  }
}
