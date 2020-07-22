// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.platform.configuration.InvalidConfigException
import com.daml.platform.sandbox.cli.Cli
import com.daml.platform.sandbox.{GlobalLogLevel, SandboxServer}
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    val config = new Cli(SandboxServer.defaultConfig).parse(args).getOrElse(sys.exit(1))
    if (!config.implicitPartyAllocation) {
      throw new InvalidConfigException(
        "This version of Sandbox does not support disabling implicit party allocation.")
    }
    config.logLevel.foreach(GlobalLogLevel.set)
    new ProgramResource(SandboxServer.owner(config)).run()
  }
}
