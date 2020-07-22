// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.platform.configuration.InvalidConfigException
import com.daml.platform.sandbox.cli.Cli
import com.daml.resources.ProgramResource

object SandboxMain {

  private val Name = LedgerName("Sandbox")

  def main(args: Array[String]): Unit = {
    new ProgramResource({
      val config = new Cli(SandboxServer.defaultConfig).parse(args).getOrElse(sys.exit(1))
      if (!config.implicitPartyAllocation) {
        throw new InvalidConfigException(
          "This version of Sandbox does not support disabling implicit party allocation.")
      }
      config.logLevel.foreach(GlobalLogLevel.set)
      SandboxServer.owner(Name, config)
    }).run()
  }

}
