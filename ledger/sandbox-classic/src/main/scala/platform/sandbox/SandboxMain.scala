// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.cliopts.GlobalLogLevel
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.resources.ProgramResource

object SandboxMain {

  def main(args: Array[String]): Unit =
    new ProgramResource({
      val config: SandboxConfig = Cli.parse(args).getOrElse(sys.exit(1))
      config.logLevel.foreach(GlobalLogLevel.set("Sandbox"))
      SandboxServer.owner(Name, config)
    }).run(ResourceContext.apply)

}
