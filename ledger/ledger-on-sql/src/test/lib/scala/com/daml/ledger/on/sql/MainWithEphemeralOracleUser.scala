// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.runner.common.Config
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource
import com.daml.testing.oracle.OracleAround

object MainWithEphemeralOracleUser {
  def main(args: Array[String]): Unit = {
    val originalConfig =
      Config
        .parse[Unit]("SQL Ledger", _ => (), (), args)
        .getOrElse(sys.exit(1))

    val user = OracleAround.createNewUniqueRandomUser()
    sys.addShutdownHook(user.drop())
    val config = originalConfig.copy(
      participants = originalConfig.participants.map(participantConfig =>
        participantConfig.copy(
          serverJdbcUrl = user.jdbcUrl,
          indexerConfig = participantConfig.indexerConfig.copy(
            haConfig = participantConfig.indexerConfig.haConfig.copy(
              indexerLockId = user.lockIdSeed,
              indexerWorkerLockId = user.lockIdSeed + 1,
            )
          ),
        )
      ),
      extra = ExtraConfig(
        // Oracle is only used as persistence for the participant; we use in-memory ledger persistence here.
        jdbcUrl = Some("jdbc:sqlite:file:test?mode=memory&cache=shared")
      ),
    )
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory, SqlConfigProvider).owner(config))
      .run(ResourceContext.apply)
  }
}
