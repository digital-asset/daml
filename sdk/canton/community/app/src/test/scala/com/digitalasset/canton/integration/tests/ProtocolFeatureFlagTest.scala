// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import monocle.macros.syntax.lens.*

class ProtocolFeatureFlagTest extends CommunityIntegrationTest with SharedEnvironment {

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.protocolFeatureFlags).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

  "Protocol feature flags" should {
    "be disabled by config" in { implicit env =>
      import env.*

      participant1.topology.synchronizer_trust_certificates
        .list(store = Some(TopologyStoreId.Synchronizer(daId)))
        .loneElement
        .item
        .featureFlags shouldBe empty
    }
  }
}
