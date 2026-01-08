// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.tests.examples.HighAvailabilityDemoTest.examplesPath

object HighAvailabilityDemoTest {
  import better.files.*
  lazy val examplesPath: File =
    "community" / "app" / "src" / "pack" / "examples" / "04-high-availability"
}

/* Use the environment variables from `postgres.conf` to configure the database access to run this test locally:
 * CANTON_DB_HOST postgres server name, default localhost
 * CANTON_DB_PORT postgres server port, default 5432
 * CANTON_DB_USER postgres db user name, default canton
 * CANTON_DB_PASSWORD password for the db user, default supersafe
 */
class HighAvailabilityDemoTest
    extends ExampleIntegrationTest(
      examplesPath / "postgres.conf",
      examplesPath / "participant-a.conf",
      examplesPath / "participant-b.conf",
      examplesPath / "mediator-a.conf",
      examplesPath / "mediator-b.conf",
      // TODO(#15837) Setup the two sequencer nodes in HA replication mode instead of BFT replication mode
      examplesPath / "sequencer-a.conf",
      examplesPath / "sequencer-b.conf",
    )
    with CommunityIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(
    UseSharedStorage.forParticipants(
      "participant_a",
      Seq("participant_b"),
      loggerFactory,
    )
  )
  registerPlugin(
    UseSharedStorage.forMediators("mediator_a", Seq("mediator_b"), loggerFactory)
  )
  // TODO(#15837) make the sequencers use shared storage again
  // registerPlugin(
  //   UseSharedStorage.forSequencers("sequencer_a", Seq("sequencer_b"), loggerFactory)
  // )

  "Run high availability demo" in { implicit env =>
    runScript(examplesPath / "admin" / "bootstrap.canton")(env.environment)
  }
}
