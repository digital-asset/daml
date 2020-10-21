// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object SandboxMain {

  def main(args: Array[String]): Unit =
    new ProgramResource({
      val config = Cli.parse(args).getOrElse(sys.exit(1))
      config.logLevel.foreach(GlobalLogLevel.set)
      SandboxServer.owner(Name, config)
    }).run(ResourceContext.apply)

}
