// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.sandbox

import com.daml.cliopts.GlobalLogLevel
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.sandbox.SandboxServer
import com.daml.platform.sandbox.config.LedgerName
import com.daml.resources.ProgramResource

import scala.util.chaining._

object SandboxMain {
  private[sandbox] val Name = LedgerName("Sandbox")

  def main(args: Array[String]): Unit =
    Cli
      .parse(args)
      .getOrElse(sys.exit(1))
      .tap(_.logLevel.foreach(GlobalLogLevel.set("Sandbox")))
      .pipe(SandboxServer.owner(Name, _))
      .pipe(new ProgramResource(_).run(ResourceContext.apply))
}
