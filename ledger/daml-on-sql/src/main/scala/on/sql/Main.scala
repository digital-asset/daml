// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.resources.ResourceContext
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.{GlobalLogLevel, SandboxServer}
import com.daml.resources.ProgramResource

object Main {

  def main(args: Array[String]): Unit = {
    val config = new Cli().parse(args).getOrElse(sys.exit(1))
    config.logLevel.foreach(GlobalLogLevel.set)
    run(config)
  }

  private[sql] def run(config: SandboxConfig): Unit =
    new ProgramResource(SandboxServer.owner(Name, config)).run(ResourceContext.apply)
}
