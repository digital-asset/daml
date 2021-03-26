// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object Main {
  val RunnerName = "sandbox-on-x"

  def main(args: Array[String]): Unit =
    new ProgramResource(
      owner = new Runner(RunnerName, BridgeLedgerFactory).owner(args)
    ).run(ResourceContext.apply)
}
