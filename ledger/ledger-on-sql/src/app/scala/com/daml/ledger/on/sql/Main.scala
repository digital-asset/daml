// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory, SqlConfigProvider).owner(args))
      .run(ResourceContext.apply)
  }
}
