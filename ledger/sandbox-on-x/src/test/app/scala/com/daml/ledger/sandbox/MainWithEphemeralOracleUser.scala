// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource
import com.daml.testing.oracle.OracleAround

object MainWithEphemeralOracleUser {
  def main(args: Array[String]): Unit = {
    val user = OracleAround.createNewUniqueRandomUser()
    sys.addShutdownHook(user.drop())
    new ProgramResource(
      owner = CliSandboxOnXRunner.owner(
        args = args,
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
      )
    ).run(ResourceContext.apply)
  }
}
