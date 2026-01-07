// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.version.ParticipantProtocolFeatureFlags

class ProtocolFeatureFlagTest extends CommunityIntegrationTest with SharedEnvironment {

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

  "Protocol feature flags" should {
    "be re-issued regardless of the state of the authorized store" in { implicit env =>
      import env.*

      val flags = ParticipantProtocolFeatureFlags.supportedFeatureFlagsByPV.getOrElse(
        testedProtocolVersion,
        Set.empty,
      )

      // It only makes sense to run this test if the PV currently tested does have feature flags
      if (flags.nonEmpty) {
        // Remove the feature flag on the synchronizer store directly
        participant1.topology.synchronizer_trust_certificates
          .propose(
            participant1.id,
            synchronizer1Id,
            store = Some(TopologyStoreId.Synchronizer(daId)),
            featureFlags = Seq.empty,
          )

        // Sanity check it's indeed gone
        participant1.topology.synchronizer_trust_certificates
          .list(store = Some(TopologyStoreId.Synchronizer(daId)))
          .loneElement
          .item
          .featureFlags shouldBe empty

        // Reconnect
        participant1.synchronizers.disconnect_all()
        participant1.synchronizers.reconnect_all()

        eventually() {
          participant1.topology.synchronizer_trust_certificates
            .list(store = Some(TopologyStoreId.Synchronizer(daId)))
            .loneElement
            .item
            .featureFlags should have size 1
        }
      }
    }
  }
}
