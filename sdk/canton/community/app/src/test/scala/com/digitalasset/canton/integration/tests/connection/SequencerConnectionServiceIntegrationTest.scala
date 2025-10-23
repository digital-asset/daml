// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.{
  SequencerConnectionValidation,
  SequencerConnectionXPool,
  SequencerConnections,
  SequencerSubscriptionPool,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level.INFO

import scala.concurrent.duration.DurationInt

sealed trait SequencerConnectionServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M1_Config
      .addConfigTransforms(
        ConfigTransforms.setConnectionPool(true),
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(config.NonNegativeDuration.tryFromDuration(2.seconds)),
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
                mediator1 -> (Seq(sequencer1, sequencer2),
                /* trust threshold */ PositiveInt.one, /* liveness margin */ NonNegativeInt.zero)
              )
            ),
          )
        )
      }

  "SequencerConnectionService" must {
    "Allow modifying the pool configuration" in { implicit env =>
      import env.*

      val connectionsConfig = Seq(sequencer1, sequencer2).map(s =>
        s.config.publicApi.clientConfig
          .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
      )

      clue("connect participant1 to all sequencers") {
        participant1.synchronizers.connect_bft(
          connections = connectionsConfig,
          sequencerTrustThreshold = PositiveInt.one,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
          validation = SequencerConnectionValidation.Disabled,
        )
      }

      participant1.health.ping(participant1.id)

      mediator1.sequencer_connection.get().value.sequencerTrustThreshold shouldBe PositiveInt.one

      clue("reconfigure mediator's trust threshold") {
        loggerFactory.assertLogsSeq(
          SuppressionRule.LevelAndAbove(INFO) && (SuppressionRule
            .forLogger[SequencerConnectionXPool] || SuppressionRule
            .forLogger[SequencerSubscriptionPool])
        )(
          mediator1.sequencer_connection.modify_connections {
            _.withSequencerTrustThreshold(PositiveInt.two).valueOrFail("set trust threshold to 2")
          },
          forExactly(2, _)(_.infoMessage should include("Configuration updated")),
        )

        mediator1.sequencer_connection.get().value.sequencerTrustThreshold shouldBe PositiveInt.two

        // The mediator is still functional
        participant1.health.ping(participant1.id)
      }

      clue("reconfigure mediator's connections to use a single connection") {
        mediator1.sequencer_connection.modify_connections { old =>
          SequencerConnections.tryMany(
            connectionsConfig.drop(1),
            sequencerTrustThreshold = PositiveInt.one,
            old.sequencerLivenessMargin,
            old.submissionRequestAmplification,
            old.sequencerConnectionPoolDelays,
          )
        }

        // The configuration has changed
        mediator1.sequencer_connection
          .get()
          .value
          .connections
          .forgetNE
          .loneElement shouldBe connectionsConfig(1)

        // The mediator is still functional
        participant1.health.ping(participant1.id)
      }

      clue("fail to reconfigure mediator's connections if validation fails") {
        sequencer1.stop()

        assertThrowsAndLogsCommandFailures(
          mediator1.sequencer_connection.modify_connections { old =>
            SequencerConnections.tryMany(
              connectionsConfig,
              sequencerTrustThreshold = PositiveInt.two,
              old.sequencerLivenessMargin,
              old.submissionRequestAmplification,
              old.sequencerConnectionPoolDelays,
            )
          },
          _.commandFailureMessage should include(
            "FAILED_PRECONDITION/TimeoutError(Connection pool failed to initialize"
          ),
        )

        // The configuration has not changed
        mediator1.sequencer_connection
          .get()
          .value
          .connections
          .forgetNE
          .loneElement shouldBe connectionsConfig(1)

        // The mediator is still functional
        // We possibly need to retry, because if participant1 has a single subscription on sequencer2, it will not detect
        // that sequencer1 is down until it first sends to it, and could therefore still pick it for the first send.
        // An alternative would be to use amplification.
        eventually() {
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            participant1.health.maybe_ping(participant1.id, timeout = 2.seconds) shouldBe defined,
            LogEntry.assertLogSeq(
              mustContainWithClue = Seq.empty,
              mayContain = Seq(
                _.warningMessage should include regex
                  raw"Request failed for server-.*\. Is the server running\? Did you configure the server address as 0\.0\.0\.0\?" +
                  raw" Are you using the right TLS settings\?"
              ),
            ),
          )
        }
      }
    }
  }
}

class SequencerConnectionServiceIntegrationTestDefault
    extends SequencerConnectionServiceIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
