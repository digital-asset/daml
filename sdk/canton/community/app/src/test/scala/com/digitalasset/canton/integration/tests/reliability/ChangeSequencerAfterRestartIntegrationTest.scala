// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalInstanceReference, LocalMediatorReference}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.SequencerConnections
import org.scalactic.source.Position
import org.slf4j.event.Level

import scala.concurrent.Future

class ChangeSequencerAfterRestartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S2M1_Manual
      .withSetup { implicit env =>
        import env.*
        Seq[LocalInstanceReference](mediator1, sequencer1, sequencer2, participant1)
          .start()

        bootstrap.synchronizer(
          synchronizerName = daName.toProtoPrimitive,
          sequencers = Seq(sequencer1, sequencer2),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, sequencer2),
          synchronizerThreshold = PositiveInt.two,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
        // we only keep the connection to sequencer1 so we can test the switch to sequencer2
        // TODO(i#16245): Right now it's not working because it's not yet supported
        mediator1.sequencer_connection.set(
          SequencerConnections.single(sequencer1.sequencerConnection)
        )
        mediator1.sequencer_connection.get() shouldBe Some(
          SequencerConnections.single(sequencer1.sequencerConnection)
        )

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        // verify it works
        clue("participant can ping itself") {
          participant1.health.maybe_ping(participant1) shouldBe defined
        }

        // shutdown nodes
        participant1.stop()
        mediator1.stop()
        sequencer1.stop()
      }

  // TODO(#16089): Currently those tests cannot work because of missing HA implementation.
  "Check that we can switch to a new sequencer" should {
    "mediator should be able to switch to new sequencer".taggedAs(mkTag("mediator")) ignore {
      implicit env =>
        import env.*
        checkCanSwitch(mediator1)
    }
    "participant should be able to switch to new sequencer".taggedAs(mkTag("participant")) ignore {
      implicit env =>
        import env.*

        // start up nodes
        clue("starting up participant") {
          participant1.start()
        }
        clue("adjusting participant sequencer connection") {
          participant1.synchronizers.modify(
            sequencer1.name,
            _.copy(sequencerConnections =
              SequencerConnections.single(sequencer2.sequencerConnection)
            ),
          )
          participant1.synchronizers.reconnect_all()
        }

        eventually() {
          participant1.health.maybe_ping(participant1) shouldBe defined
        }

    }
  }

  private def mkTag(component: String) = ReliabilityTest(
    Component(component, "connected to sequencer"),
    AdverseScenario(
      dependency = "sequencer",
      details = s"changes connection while $component is shut down",
    ),
    Remediation(
      remediator = "mutable sequencer connection",
      action = s"$component is manually reconfigured",
    ),
    outcome = s"$component can reconnect and resume",
  )

  private val expectedWarnings = LogEntry.assertLogSeq(
    Seq.empty,
    Seq(
      _.message should (include("Is the server running")),
      _.message should (include("Is the server initialized")),
      _.message should (include("Unable to connect to sequencer")),
    ),
  ) _

  private def checkCanSwitch(
      node: LocalMediatorReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    // start up nodes
    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        // To prevent racy tests we need to block until the mediator is up.
        val startupF = Future {
          node.start()
        }

        clue(s"adjusting ${node.name} sequencer connection") {
          eventually() {
            node.sequencer_connection.set(
              SequencerConnections.single(sequencer2.sequencerConnection)
            )
          }
        }
        clue(s"${node.name} startup eventually completes") {
          val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(2))
          startupF.futureValue(patience, Position.here)
        }
      },
      expectedWarnings,
    )

    clue(s"${node.name} doesn't struggle with invalid connections") {
      assertThrowsAndLogsCommandFailures(
        node.sequencer_connection.set(
          SequencerConnections.single(sequencer1.sequencerConnection)
        ),
        _.errorMessage should include("Unable to connect to sequencer at"),
      )
    }
  }

}
