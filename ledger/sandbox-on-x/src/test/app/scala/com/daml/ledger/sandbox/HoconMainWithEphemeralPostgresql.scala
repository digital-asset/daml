// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.HoconCli
import com.daml.testing.postgresql.PostgresAround

object HoconMainWithEphemeralPostgresql extends PostgresAround {

  def main(args: Array[String]): Unit = {
    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    System.setProperty("DEFAULT_PARTICIPANT_DATABASE_JDBC_URL", database.url)
    SandboxOnXRunner.run(HoconCli.loadConfigWithOverrides(SandboxOnXRunner.RunnerName, args))
  }
}
