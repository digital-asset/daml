// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.util.ShowUtil.*

trait DistributedStatusIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with StatusIntegrationTestUtil {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual

  private def assertNotStarted[Ref <: InstanceReference](
      failureMap: Map[String, NodeStatus.Failure],
      node: Ref,
  ): Unit = {
    failureMap should have size 1
    failureMap(node.name).msg should include(
      show"$node has not been started."
    )
  }

  private def assertNotInitialized[Ref <: InstanceReference](
      failureMap: Map[String, NodeStatus.Failure],
      node: Ref,
  ): Unit = {
    failureMap should have size 1
    failureMap(node.name).msg should include(
      show"$node has not been initialized"
    )
  }

  private def assertNodeIsHealthy[Ref <: InstanceReference](
      map: Map[String, _ <: NodeStatus.Status],
      node: Ref,
  ) = {
    map should have size 1
    val status = map(node.name)
    status.uid.identifier.str shouldBe "mediator1"
    status.active shouldBe true
    status.uptime.toNanos should be > 0L
  }

  "environment using an external sequencer" should {
    "still be able to ping a participant" in { implicit env =>
      import env.*

      {
        val sts = health.status()
        sts.participantStatus shouldBe empty
        sts.mediatorStatus shouldBe empty
        sts.sequencerStatus shouldBe empty

        assertNotStarted(sts.unreachableMediators, mediator1)
        assertNotStarted(sts.unreachableSequencers, sequencer1)
        forEvery(sts.unreachableParticipants) { case (ref, failure) =>
          failure.msg should include(show"Instance Participant '$ref' has not been started.")
        }
        sts.unreachableParticipants should have size participants.all.size.toLong
      }

      participants.local.start()
      sequencers.local.start()
      mediators.local.start()

      {
        val sts = health.status()
        sts.sequencerStatus shouldBe empty
        sts.mediatorStatus shouldBe empty

        assertNotInitialized(sts.unreachableMediators, mediator1)
        assertNotInitialized(sts.unreachableSequencers, sequencer1)
        sts.participantStatus should have size participants.all.size.toLong
        assertP1UnconnectedStatus(sts.participantStatus(participant1.name), testedProtocolVersion)
      }

      bootstrap
        .synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )

      eventually() {
        val sts = health.status()
        sts.unreachableMediators shouldBe empty
        sts.unreachableSequencers shouldBe empty

        assertNodeIsHealthy(sts.mediatorStatus, mediator1)
        assertMediatorVersioningInfo(sts.mediatorStatus(mediator1.name), testedProtocolVersion)

        sts.sequencerStatus should have size 1
        assertSequencerUnconnectedStatus(
          sts.sequencerStatus(sequencer1.name),
          connectedMediators = List(mediator1.id),
          daName,
          testedProtocolVersion,
        )
      }

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)

      eventually() {
        val sts = health.status()
        assertNodesAreConnected(
          sts.sequencerStatus(sequencer1.name),
          sts.participantStatus(participant1.name),
          connectedMediators = List(mediator1.id),
          testedProtocolVersion,
        )
      }

      // manually stop all nodes so the sequencer will have no active connections and can shutdown cleanly
      participants.local.stop()
    }
  }
}

class DistributedStatusReferenceIntegrationTestPostgres extends DistributedStatusIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class DistributedStatusBftOrderingIntegrationTestPostgres extends DistributedStatusIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
