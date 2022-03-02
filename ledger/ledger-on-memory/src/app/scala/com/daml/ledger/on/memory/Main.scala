// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.runner.common.Config
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    val resource = for {
      config <- Config.ownerWithoutExtras(RunnerName, args)
      owner <- Owner(config)
    } yield owner
    new ProgramResource(resource).run(ResourceContext.apply)
  }
}
