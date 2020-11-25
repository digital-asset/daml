// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit =
    IntegrityChecker.run(args, commitStrategySupportFactory _)

  private def commitStrategySupportFactory(executionContext: ExecutionContext) =
    new LogAppendingCommitStrategySupport()(executionContext)
}
