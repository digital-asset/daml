// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource
import com.daml.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    new ProgramResource(
      owner = CliSandboxOnXRunner.owner(
        args = args,
        manipulateConfig = originalConfig =>
          originalConfig.copy(
            participants = originalConfig.participants.map(p =>
              p.copy(
                serverJdbcUrl = database.url
              )
            )
          ),
      )
    ).run(ResourceContext.apply)
  }
}
