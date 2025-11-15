// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.{ComponentHealthState, ComponentStatus}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{InstanceReference, LocalInstanceReference}
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
import com.digitalasset.canton.sequencing.{
  SequencerConnectionValidation,
  SubmissionRequestAmplification,
}

/** This test checks that the sequencer connection pool properly reports the status of its
  * components through the health status API.
  */
sealed trait ConnectionPoolHealthIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .addConfigTransforms(
        ConfigTransforms.setConnectionPool(true)
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.local,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (sequencers.local,
                /* trust threshold */ PositiveInt.two, /* liveness margin */ NonNegativeInt.one)
              )
            ),
          )
        )
      }

  "Member nodes" must {
    "Initialize the setup" in { implicit env =>
      import env.*

      val connectionsConfig = sequencers.local.map(s =>
        s.config.publicApi.clientConfig
          .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
      )

      clue("connect participants to all sequencers") {
        participants.local.foreach { participant =>
          participant.start()
          participant.synchronizers.connect_bft(
            connections = connectionsConfig,
            sequencerTrustThreshold = PositiveInt.two,
            sequencerLivenessMargin = NonNegativeInt.one,
            submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
            synchronizerAlias = daName,
            physicalSynchronizerId = Some(daId),
            validation = SequencerConnectionValidation.Disabled,
          )
        }
      }

      clue("ping") {
        participant1.health.ping(participant2.id)
      }
    }

    "properly report the health of the connection pool components" in { implicit env =>
      import env.*

      clue("initial status") {
        // eventually() to allow for delays in having all the connections and subscriptions up
        eventually() {
          val nodes = (participants.local: Seq[LocalInstanceReference]) ++ mediators.local
          nodes.foreach { node =>
            val (connectionPool, connections, subscriptionPool, subscriptions) =
              getHealthComponents(node)

            connections should have size 4
            subscriptions should have size 3 // trust threshold + liveness margin
            forAll(Seq(connectionPool, subscriptionPool) ++ connections ++ subscriptions)(
              _.state shouldBe ComponentHealthState.Ok()
            )
          }
        }
      }

      clue("stop some sequencers") {
        Seq(sequencer1, sequencer2).foreach(_.stop())
      }

      clue("status after some sequencers are lost") {
        eventually() {
          val nodes = (participants.local: Seq[LocalInstanceReference]) ++ mediators.local
          nodes.foreach { node =>
            val (connectionPool, connections, subscriptionPool, subscriptions) =
              getHealthComponents(node)

            connections should have size 4
            // The subscription pool will pick extra as needed but will be limited to 2
            subscriptions should have size 2

            // The connection pool and subscriptions should be Ok
            forAll(connectionPool +: subscriptions)(
              _.state shouldBe ComponentHealthState.Ok()
            )

            // The subscription pool should be degraded (below liveness margin)
            subscriptionPool.state match {
              case ComponentHealthState.Degraded(state) =>
                state.description.value should include("below liveness margin")
              case _ => fail("subscription pool should be degraded")
            }

            // Connections to sequencers 1 and 2 should be Failed, connections to sequencers 3 and 4 should be Ok
            forAll(connections) {
              case ComponentStatus(name, ComponentHealthState.Failed(state)) =>
                name should (include("sequencer1") or include("sequencer2"))
                state.description.value should startWith("Network error")
              case ComponentStatus(name, ComponentHealthState.Ok(_)) =>
                name should (include("sequencer3") or include("sequencer4"))
              case other => fail(s"Unexpected component status: $other")
            }
          }
        }
      }
    }

    def getHealthComponents(
        node: LocalInstanceReference
    ): (ComponentStatus, Seq[ComponentStatus], ComponentStatus, Seq[ComponentStatus]) = {
      val components = node.health.status.trySuccess.components

      val connectionPool = components.filter(_.name == "sequencer-connection-pool").loneElement
      val connections = components.filter(_.name.startsWith("internal-sequencer-connection-"))

      val subscriptionPool =
        components.filter(_.name == "sequencer-subscription-pool").loneElement
      val subscriptions =
        components.filter(_.name.startsWith("subscription-sequencer-connection-"))

      (connectionPool, connections, subscriptionPool, subscriptions)
    }
  }
}

class ConnectionPoolHealthIntegrationTestDefault extends ConnectionPoolHealthIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
