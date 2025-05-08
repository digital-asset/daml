// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.console.commands.SynchronizerChoice
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality

import scala.concurrent.duration.*

trait SimpleFunctionalNodesTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with NodeTestingUtils {

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  private val topologyTransactionRegistrationTimeout =
    config.NonNegativeDuration.tryFromDuration(5.seconds)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        // Lower SynchronizerOutbox/SynchronizerTopologyService retry interval so default eventual does not time out after sequencer-x stop/start:
        ConfigTransforms.setTopologyTransactionRegistrationTimeout(
          topologyTransactionRegistrationTimeout
        )*
      )

  "Connect participant-x" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
  }

  "Temporarily stop sequencer and participant re-send topology after restart" in { implicit env =>
    import env.*

    loggerFactory.assertLogsUnorderedOptional(
      {
        // Stop the only sequencer
        stopAndWait(sequencer1)

        // Have the participant struggle to publish party creation.
        participant1.parties
          .enable("partyToSeeAfterSequencerRestart", waitForSynchronizer = SynchronizerChoice.None)

        // Then restart the sequencer
        startAndWait(sequencer1)

        // wait for the 1.5 * topology transaction registration timeout, so that we are sure we hit the timeout
        Threading.sleep((topologyTransactionRegistrationTimeout.duration * 1.5).toMillis)

        // Trigger a new event on the synchronizer so that SendTracker can time out the topology broadcast message
        participant1.testing.fetch_synchronizer_time(daId)

        // Eventually the participant SynchronizerOutbox should succeed at resending the party creation.
        eventually() {
          val parties = participant1.parties.list("partyToSeeAfterSequencerRestart")
          parties should have size (1)
        }
      },
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include("Request failed for sequencer. Is the server running?"),
      ),
      (
        LogEntryOptionality.Required,
        _.warningMessage should include("failed the following topology transactions"),
      ),
    )
  }

  "Allocate the same party twice" in { implicit env =>
    import env.*

    participant1.ledger_api.parties.allocate("partyCannotRecreate")
    eventually() {
      val parties = participant1.parties.list("partyCannotRecreate")
      parties should have size (1)
    }

    logger.info("About to allocate the same party a second time")

    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.ledger_api.parties.allocate("partyCannotRecreate")
      ),
      _.commandFailureMessage should include regex
        s"GrpcClientError: INVALID_ARGUMENT/INVALID_ARGUMENT\\(8,........\\): The submitted request has invalid arguments: Party already exists: party partyCannotRecreate::.*\\.\\.\\. is already allocated on this node",
    )
  }

}

class SimpleFunctionalNodesTestH2 extends SimpleFunctionalNodesTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

class SimpleFunctionalNodesTestPostgres extends SimpleFunctionalNodesTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
