// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.platform.sandbox.cli.Cli
import com.daml.platform.sandbox.config.InvalidConfigException
import com.daml.resources.ProgramResource

object SandboxMain {
  def main(args: Array[String]): Unit = {
    val config = Cli.parse(args).getOrElse(sys.exit(1))
    if (!config.implicitPartyAllocation) {
      throw new InvalidConfigException(
        "This version of Sandbox does not support disabling implicit party allocation.")
    }
    config.logLevel.foreach(GlobalLogLevel.set)
    new ProgramResource(SandboxServer.owner(config)).run()
  }
}
