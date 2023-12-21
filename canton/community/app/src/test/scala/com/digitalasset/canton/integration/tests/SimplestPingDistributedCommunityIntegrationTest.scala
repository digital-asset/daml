// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}

class SimplestPingDistributedCommunityIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {
  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition
      .fromResource("distributed-single-domain.conf")
      .addConfigTransforms(CommunityConfigTransforms.uniquePorts)
      .withManualStart

  "we can run a trivial ping" in { implicit env =>
    import env.*

    mediator1x.start()

    participants.local.start()

    mediator1x.health.status shouldBe NodeStatus.NotInitialized(true)

  // TODO(i15178): test this as soon as the mediator can be initialized
  // mediator1x.testing.fetch_domain_time()

  // TODO(i15178): add sequencer and domain manager, then test if the distributed domain can be used for a ping
  }

}
