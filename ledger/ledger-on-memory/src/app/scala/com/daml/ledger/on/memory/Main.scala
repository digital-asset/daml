// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    val resource = for {
      config <- Config.owner(
        RunnerName,
        InMemoryLedgerFactory.extraConfigParser,
        InMemoryLedgerFactory.defaultExtraConfig,
        args,
      )
      owner <- Owner(config)
    } yield owner
    new ProgramResource(resource).run(ResourceContext.apply)
  }
}
