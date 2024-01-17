// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.CommunityStorageConfig
import com.digitalasset.canton.console.InstanceReferenceX
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}

sealed trait SimplestPingXCommunityIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopologyX
      .addConfigTransforms(CommunityConfigTransforms.uniquePorts)
      .withManualStart

  "we can run a trivial ping" in { implicit env =>
    import env.*

    sequencer1x.start()
    mediator1x.start()

    sequencer1x.health.status shouldBe NodeStatus.NotInitialized(true)
    mediator1x.health.status shouldBe NodeStatus.NotInitialized(true)

    bootstrap.domain(
      "da",
      Seq(sequencer1x),
      Seq(mediator1x),
      Seq[InstanceReferenceX](sequencer1x, mediator1x),
    )

    sequencer1x.health.status shouldBe a[NodeStatus.Success[?]]
    mediator1x.health.status shouldBe a[NodeStatus.Success[?]]

    participantsX.local.start()

    participantsX.local.domains.connect_local(sequencer1x)
    mediator1x.testing
      .fetch_domain_time() // Test if the DomainTimeService works for community mediators as well.
    participant1x.health.ping(participant2x)
  }
}

final class SimplestPingReferenceXCommunityIntegrationTest
    extends SimplestPingXCommunityIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[CommunityStorageConfig.Memory](loggerFactory)
  )
}
