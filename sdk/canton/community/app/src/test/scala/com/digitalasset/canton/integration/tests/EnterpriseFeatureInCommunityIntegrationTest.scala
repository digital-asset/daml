// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.{NodeStatus, WaitingForInitialization}
import com.digitalasset.canton.config.CommunityStorageConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.participant.admin.ResourceLimits

sealed trait EnterpriseFeatureInCommunityIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  private val domainAlias = "da"

  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopology
      .addConfigTransforms(CommunityConfigTransforms.uniquePorts)
      .addConfigTransforms(CommunityConfigTransforms.setProtocolVersion(testedProtocolVersion)*)
      .withManualStart
      .withSetup { implicit env =>
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

        bootstrap.domain(
          domainAlias,
          Seq(sequencer1),
          Seq(mediator1),
          Seq[InstanceReference](sequencer1, mediator1),
          PositiveInt.two,
          staticDomainParameters = CommunityEnvironmentDefinition.defaultStaticDomainParameters,
        )

        sequencer1.health.wait_for_initialized()
        mediator1.health.wait_for_initialized()

        sequencer1.health.status shouldBe a[NodeStatus.Success[?]]
        mediator1.health.status shouldBe a[NodeStatus.Success[?]]
      }

  "setting participant resource limits in community should fail gracefully" in { implicit env =>
    import env.*

    participant1.start()
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.resources.set_resource_limits(
        ResourceLimits(Some(NonNegativeInt.one), Some(NonNegativeInt.one))
      ),
      _.errorMessage should include(
        "This method is unsupported by the Community edition of canton."
      ),
    )
  }
}

final class EnterpriseFeatureInCommunityReferenceIntegrationTest
    extends EnterpriseFeatureInCommunityIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[CommunityStorageConfig.Memory](loggerFactory)
  )
}
