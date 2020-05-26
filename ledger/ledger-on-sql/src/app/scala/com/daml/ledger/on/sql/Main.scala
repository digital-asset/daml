// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory).owner(args)).run()
  }
}
