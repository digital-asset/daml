// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.either.*
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.logging.SuppressionRule
import org.scalatest.Ignore
import org.slf4j.event.Level

// TODO(i21038): Change the test to run on CI without requiring docker
@Ignore
class PostgresReadOnlyIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  // Need to force the use of a testcontainer as we set the entire postgres system to read-only and that affects other tests using the same service container
  private val postgres = new UsePostgres(loggerFactory, forceTestContainer = true)

  registerPlugin(postgres)

  private def setDefaultTransaction(readOnly: Boolean): Unit = {
    val storage = postgres.dbSetup.storage
    import storage.api.*

    logger.info(s"Setting default transaction read-only to $readOnly")

    // Set connections to read-only by default and reload config
    storage
      .runWrite(
        sqlu"ALTER SYSTEM SET default_transaction_read_only TO #$readOnly; SELECT pg_reload_conf();",
        "set-read-only",
        0,
      )
      .futureValueUS
  }

  override def afterAll(): Unit =
    try {
      // Make sure to disable read-only connections again when a test had failed before dropping the db
      setDefaultTransaction(false)
    } finally super.afterAll()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

  "participant is connected and health" in { env =>
    import env.*

    assertPingSucceeds(participant1, participant1)
  }

  "nodes recover when postgres db becomes temporarily read-only" in { env =>
    import env.*

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      {
        setDefaultTransaction(true)

        // Ping must fail after connections became read-only
        Either
          .catchOnly[CommandFailure] {
            participant1.health.ping(participant1)
          }
          .left
          .value
          .getMessage should include("Command execution failed")

        // Allow read-write transactions again
        setDefaultTransaction(false)

        // Eventually system is healthy again
        eventually() {
          Either
            .catchOnly[CommandFailure](
              participant1.health.maybe_ping(participant1)
            )
            .toOption
            .flatten shouldBe defined
        }
      },
      logs =>
        forAtLeast(1, logs) { log =>
          log.throwable match {
            case Some(entry) =>
              // The INSERT error can appear either in the throwable itself, or in its cause. Either way, the
              // Throwable message will contain it.
              entry.getMessage should include(
                "ERROR: cannot execute INSERT in a read-only transaction"
              )
            // Read-only transaction does not always show up, sometimes the node becomes passive before that.
            case None => log.message should include("NODE_IS_PASSIVE_REPLICA")
          }
        },
    )

  }

}
