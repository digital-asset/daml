// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.{NodeStatus, WaitingForInitialization}
import com.digitalasset.canton.config.CommunityStorageConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}

sealed trait SimplestPingCommunityIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopology
      .addConfigTransforms(CommunityConfigTransforms.uniquePorts)
      .addConfigTransforms(CommunityConfigTransforms.setProtocolVersion(testedProtocolVersion)*)
      .withManualStart

  "we can run a trivial ping" in { implicit env =>
    import env.*

    sequencer1.start()
    mediator1.start()

    sequencer1.health.status shouldBe NodeStatus.NotInitialized(
      active = true,
      Some(WaitingForInitialization),
    )
    mediator1.health.status shouldBe NodeStatus.NotInitialized(
      active = true,
      Some(WaitingForInitialization),
    )

    bootstrap.synchronizer(
      "da",
      Seq(sequencer1),
      Seq(mediator1),
      Seq[InstanceReference](sequencer1, mediator1),
      PositiveInt.two,
      staticSynchronizerParameters =
        CommunityEnvironmentDefinition.defaultStaticSynchronizerParameters,
    )

    sequencer1.health.status shouldBe a[NodeStatus.Success[?]]
    mediator1.health.status shouldBe a[NodeStatus.Success[?]]

    participants.local.start()

    participants.local.synchronizers.connect_local(sequencer1, "da")
    mediator1.testing
      .fetch_domain_time() // Test if the DomainTimeService works for community mediators as well.
    participant1.health.ping(participant2)
  }
}

// If this test is renamed, update `propose-open-source-code-drop.sh`
final class SimplestPingReferenceCommunityIntegrationTest
    extends SimplestPingCommunityIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[CommunityStorageConfig.Memory](loggerFactory)
  )
}
