// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.{Config, Runner}
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource
import com.daml.testing.oracle.OracleAround

// TODO for Brian: please verify usage of OracleAround here
object MainWithEphemeralOracleUser extends OracleAround {
  def main(args: Array[String]): Unit = {
    val originalConfig =
      Config
        .parse[Unit]("SQL Ledger", _ => (), (), args)
        .getOrElse(sys.exit(1))

    connectToOracle()
    val user = createNewRandomUser()
    sys.addShutdownHook(dropUser(user.name))
    val oracleJdbcUrl = s"jdbc:oracle:thin:${user.name}/${user.pwd}@localhost:$oraclePort/ORCLPDB1"
    val config = originalConfig.copy(
      participants = originalConfig.participants.map(_.copy(serverJdbcUrl = oracleJdbcUrl)),
      extra = ExtraConfig(jdbcUrl =
        Some("jdbc:h2:mem:ledger-on-sql-conformance-test")
      ), // Oracle is only used as persistence for participant, we use in-memory H2 for sql ledger persistence here
    )
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory).owner(config))
      .run(ResourceContext.apply)
  }
}
