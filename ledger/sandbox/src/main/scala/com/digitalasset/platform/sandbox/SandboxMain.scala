// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.resources.ProgramResource

object SandboxMain {
  def main(args: Array[String]): Unit = {
    val config = Cli.parse(args).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set)
    new ProgramResource(SandboxServer.owner(config)).run()
  }
}
