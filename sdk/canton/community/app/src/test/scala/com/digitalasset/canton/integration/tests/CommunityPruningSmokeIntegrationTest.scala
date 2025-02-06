// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.{NodeStatus, WaitingForInitialization}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StorageConfig
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

/** The objective of this test is to verify that pruning is accessible in the community edition
  * and no longer fails with a NotSupportedInCommunityEdition error.
  */
sealed trait CommunityPruningSmokeIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  private val synchronizerAlias = "da"

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

        bootstrap.synchronizer(
          synchronizerAlias,
          Seq(sequencer1),
          Seq(mediator1),
          Seq[InstanceReference](sequencer1, mediator1),
          PositiveInt.two,
          staticSynchronizerParameters =
            CommunityEnvironmentDefinition.defaultStaticSynchronizerParameters,
        )

        sequencer1.health.wait_for_initialized()
        mediator1.health.wait_for_initialized()

        sequencer1.health.status shouldBe a[NodeStatus.Success[?]]
        mediator1.health.status shouldBe a[NodeStatus.Success[?]]
      }

  "mediator pruning should be accessible in the community edition" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      mediator1.pruning.prune(),
      // The mediator complaining about lack of pruning data is a sign that pruning is not disabled
      _.errorMessage should include("There is no mediator data available for pruning"),
    )

    // Sanity check that the mediator pruning scheduler is also accessible.
    mediator1.pruning.clear_schedule()
  }

  "sequencer pruning should be accessible in the community edition" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      sequencer1.pruning.prune(),
      // The sequencer complaining about unsafe pruning point is a sign that pruning is not disabled
      _.errorMessage should include regex
        "GrpcRequestRefusedByServer: FAILED_PRECONDITION/Could not prune at .* as the earliest safe pruning point is",
    )

    // TODO(#15987): Block sequencer does not yet support scheduled pruning
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      sequencer1.pruning.clear_schedule(),
      _.errorMessage should include(
        "GrpcServiceUnavailable: UNIMPLEMENTED/This sequencer does not support scheduled pruning"
      ),
    )
  }

  "participant pruning should be accessible in the community edition" in { implicit env =>
    import env.*

    participant1.start()
    participant1.synchronizers.connect_local(
      sequencer1,
      alias = SynchronizerAlias.tryCreate(synchronizerAlias),
    )

    // Generate some data to have something to prune
    participant1.health.ping(participant1)

    val safeOffset =
      participant1.pruning.find_safe_offset().getOrElse(fail("Expected to find offset"))
    logger.info(
      s"Managed to call find_safe_offset on community participant and obtained safe offset $safeOffset"
    )
    participant1.pruning.prune(safeOffset)
    logger.info("Managed to call prune on community participant")
    participant1.pruning.prune_internally(safeOffset)
    logger.info("Managed to call prune_internally on community participant")

    // Sanity check that the participant pruning scheduler is also accessible.
    participant1.pruning.clear_schedule()
  }
}

final class CommunityReferencePruningSmokeIntegrationTest
    extends CommunityPruningSmokeIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory)
  )
}
