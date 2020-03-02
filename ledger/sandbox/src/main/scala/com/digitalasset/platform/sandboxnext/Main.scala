// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import com.digitalasset.platform.sandbox.GlobalLogLevel
import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    val config = Cli.parse(args, SandboxConfig.nextDefault).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set)
    new ProgramResource(new Runner().owner(config)).run()
  }
}
