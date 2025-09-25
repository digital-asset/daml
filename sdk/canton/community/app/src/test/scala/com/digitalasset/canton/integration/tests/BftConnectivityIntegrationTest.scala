// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*

sealed trait BftConnectivityIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M1_Config
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          _.focus(_.maxConnectionRetryDelay).replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        )
      )
      .withNetworkBootstrap { implicit env =>
        import env.*

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, sequencer2),
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (Seq(sequencer1, sequencer2), /* trust threshold */ PositiveInt.two)
              )
            ),
          )
        )
      }

  "A participant connected to a BFT synchronizer" when {
    "starting up with sequencers down" must {
      "retry the connection until successful" in { implicit env =>
        import env.*

        clue("Connect to the synchronizer") {
          participant1.synchronizers.connect_bft(
            Seq(sequencer1, sequencer2).map(s =>
              s.config.publicApi.clientConfig
                .asSequencerConnection(SequencerAlias.tryCreate(s.name))
            ),
            sequencerTrustThreshold = PositiveInt.two,
            synchronizerAlias = daName,
          )
        }

        val retryLog =
          raw"Skipping failing synchronizer Synchronizer 'synchronizer1' after FAILED_TO_CONNECT_TO_SEQUENCERS\(.*\):" +
            raw" The participant failed to connect to the sequencers. Will schedule subsequent retry.".r

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            clue("Stop participant") {
              participant1.stop()
            }

            clue("Stop sequencers") {
              sequencer1.stop()
              sequencer2.stop()
            }

            clue("Restart participant") {
              participant1.start()
              // A normal startup of the Canton application triggers an automatic reconnect, but here we must trigger it manually
              participant1.synchronizers.reconnect_all()
            }

            // Wait for the retry announcement
            eventually() {
              val recordedLogEntries = loggerFactory.fetchRecordedLogEntries
              recordedLogEntries.exists(_.warningMessage.matches(retryLog)) shouldBe true
            }
            logger.debug(s"Observed retry announcement")

            clue("Start sequencers") {
              sequencer1.start()
              sequencer2.start()
            }

            clue("Participant reconnects") {
              eventually() {
                participant1.synchronizers.list_connected().loneElement
              }
            }

            clue("Participant can ping itself") {
              participant1.health.ping(participant1)
            }
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq(
              (
                _.warningMessage should include regex retryLog,
                "participant will retry to connect",
              )
            )
          ),
        )
      }
    }
  }
}

class BftConnectivityIntegrationTestDefault extends BftConnectivityIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
