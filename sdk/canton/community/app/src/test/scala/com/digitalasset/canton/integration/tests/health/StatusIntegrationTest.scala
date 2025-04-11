// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, IdentityConfig}
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
  IsolatedEnvironments,
}
import com.digitalasset.canton.util.ShowUtil.*
import monocle.macros.syntax.lens.*

sealed trait StatusIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with StatusIntegrationTestUtil {
  // Two synchronizers (da and acme); da has auto-init enabled and acme has auto-init disabled
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 2,
        numMediators = 2,
      )
      .withManualStart
      .addConfigTransform(
        ConfigTransforms.updateMediatorConfig("mediator2")(
          _.focus(_.init.identity).replace(IdentityConfig.Manual)
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateSequencerConfig("sequencer2")(
          _.focus(_.init.identity).replace(IdentityConfig.Manual)
        )
      )

  "aggregate status should provide all reachable nodes' statuses " in { implicit env =>
    import env.*
    {
      val sts = health.status()
      sts.participantStatus shouldBe empty

      forEvery(sts.unreachableParticipants) { case (ref, failure) =>
        failure.msg should include(show"Participant '$ref' has not been started.")
      }
      sts.unreachableParticipants should have size 2
    }

    startAll()

    NetworkBootstrapper(
      Seq(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
        )
      )
    ).bootstrap()

    {
      val sts = health.status()
      sts.sequencerStatus should have size 1 // uninitialized sequencer of acme will not appear
      sts.mediatorStatus should have size 1 // uninitialized mediator of acme will not appear
      sts.participantStatus should have size 2

      sts.unreachableSequencers(sequencer2.name).msg should startWith(
        "Sequencer 'sequencer2' has not been initialized"
      )
      sts.unreachableMediators(mediator2.name).msg should startWith(
        "Mediator 'mediator2' has not been initialized"
      )

      assertP1UnconnectedStatus(sts.participantStatus(participant1.name), testedProtocolVersion)
      eventually() {
        assertSequencerUnconnectedStatus(
          health.status().sequencerStatus(sequencer1.name),
          connectedMediators = List(mediator1.id),
          daName,
          testedProtocolVersion,
        )
      }
    }

    {
      mediator1.stop()
      assertSequencerUnconnectedStatus(
        sequencer1.health.status.trySuccess,
        connectedMediators = Nil,
        daName,
        testedProtocolVersion,
      )

      mediator1.start()
      eventually() {
        assertSequencerUnconnectedStatus(
          sequencer1.health.status.trySuccess,
          connectedMediators = List(mediator1.id),
          daName,
          testedProtocolVersion,
        )
      }
    }

    participant1.synchronizers.connect_local(sequencer1, alias = daName)

    eventually() {
      val sts = health.status()

      assertNodesAreConnected(
        sts.sequencerStatus(sequencer1.name),
        sts.participantStatus(participant1.name),
        connectedMediators = List(mediator1.id),
        testedProtocolVersion,
      )
    }
  }

  "also possible to call status individually" in { implicit env =>
    import env.*

    inside(sequencer1.health.status) { case f: NodeStatus.Failure =>
      f.msg should include("Sequencer 'sequencer1' has not been started.")
    }

    inside(participant1.health.status) { case f: NodeStatus.Failure =>
      f.msg should include("Participant 'participant1' has not been started.")
    }

    startAll()

    NetworkBootstrapper(
      Seq(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
        )
      )
    ).bootstrap()

    assertP1UnconnectedStatus(participant1.health.status.trySuccess, testedProtocolVersion)
    eventually() {
      assertSequencerUnconnectedStatus(
        sequencer1.health.status.trySuccess,
        connectedMediators = List(mediator1.id),
        daName,
        testedProtocolVersion,
      )
    }

    participant1.synchronizers.connect_local(sequencer1, alias = daName)

    eventually() {
      assertNodesAreConnected(
        sequencer1.health.status.trySuccess,
        participant1.health.status.trySuccess,
        connectedMediators = List(mediator1.id),
        testedProtocolVersion,
      )
    }
  }
}

class StatusReferenceIntegrationTestPostgres extends StatusIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
