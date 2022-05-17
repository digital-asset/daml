// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.HoconCli
import com.daml.testing.oracle.OracleAround

object HoconMainWithEphemeralOracleUser {
  def main(args: Array[String]): Unit = {
    val user = OracleAround.createNewUniqueRandomUser()
    sys.addShutdownHook(user.drop())
    System.setProperty("DEFAULT_PARTICIPANT_DATABASE_JDBC_URL", user.jdbcUrl)
    System.setProperty("INDEXER_HIGH_AVAILABILITY_LOCK_ID", (user.lockIdSeed).toString)
    System.setProperty("INDEXER_HIGH_AVAILABILITY_WORKER_LOCK_ID", (user.lockIdSeed + 1).toString)
    SandboxOnXRunner.run(HoconCli.loadConfigWithOverrides(SandboxOnXRunner.RunnerName, args))
  }
}
