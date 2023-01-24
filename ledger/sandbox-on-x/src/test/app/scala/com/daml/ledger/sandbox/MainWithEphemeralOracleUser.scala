// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.testing.oracle.OracleAround

object MainWithEphemeralOracleUser {
  def main(args: Array[String]): Unit = {
    val user = OracleAround.createNewUniqueRandomUser()
    sys.addShutdownHook(user.drop())
    System.setProperty("DEFAULT_PARTICIPANT_DATABASE_JDBC_URL", user.jdbcUrl)
    System.setProperty("INDEXER_HIGH_AVAILABILITY_LOCK_ID", (user.lockIdSeed).toString)
    System.setProperty("INDEXER_HIGH_AVAILABILITY_WORKER_LOCK_ID", (user.lockIdSeed + 1).toString)
    CliSandboxOnXRunner.run(
      args,
      manipulateConfig = originalConfig =>
        originalConfig.copy(
          participants = originalConfig.participants.map(participantConfig =>
            participantConfig.copy(
              serverJdbcUrl = user.jdbcUrl,
              indexerConfig = participantConfig.indexerConfig.copy(
                highAvailability = participantConfig.indexerConfig.highAvailability.copy(
                  indexerLockId = user.lockIdSeed,
                  indexerWorkerLockId = user.lockIdSeed + 1,
                )
              ),
            )
          )
        ),
      registerGlobalOpenTelemetry = false,
    )
  }
}
