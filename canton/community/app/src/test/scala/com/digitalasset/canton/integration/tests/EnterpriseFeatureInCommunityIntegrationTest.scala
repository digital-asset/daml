// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.CommunityStorageConfig
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
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
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.PruningNotSupportedInCommunityEdition
import com.digitalasset.canton.platform.apiserver.services.ApiConversions

sealed trait EnterpriseFeatureInCommunityXIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  private val domainAlias = "da"

  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopologyX
      .addConfigTransforms(
        CommunityConfigTransforms.uniquePorts
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        sequencer1x.start()
        mediator1x.start()

        sequencer1x.health.status shouldBe NodeStatus.NotInitialized(true)
        mediator1x.health.status shouldBe NodeStatus.NotInitialized(true)

        bootstrap.domain(
          domainAlias,
          Seq(sequencer1x),
          Seq(mediator1x),
          Seq[InstanceReference](sequencer1x, mediator1x),
        )

        sequencer1x.health.wait_for_initialized()
        mediator1x.health.wait_for_initialized()

        sequencer1x.health.status shouldBe a[NodeStatus.Success[?]]
        mediator1x.health.status shouldBe a[NodeStatus.Success[?]]
      }

  "sequencer and mediator enterprise admin commands should gracefully fail" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      sequencer1x.pruning.prune(),
      // logged at the server
      logentry =>
        logentry.warningMessage should include(
          "This Community edition of canton does not support the operation: SequencerPruningAdministrationService.Prune."
        ),
      // logged at the client
      logentry =>
        logentry.commandFailureMessage should include(
          "unsupported by the Community edition of canton"
        ),
    )

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      mediator1x.pruning.prune(),
      // logged at the server
      logentry =>
        logentry.warningMessage should include(
          "This Community edition of canton does not support the operation: MediatorAdministrationService.Prune."
        ),
      // logged at the client
      logentry =>
        logentry.commandFailureMessage should include(
          "unsupported by the Community edition of canton"
        ),
    )
  }

  "participant pruning should fail gracefully" in { implicit env =>
    import env.*

    participant1x.start()
    participant1x.domains.connect_local(
      sequencer1x,
      alias = DomainAlias.tryCreate(domainAlias),
    )

    val startOffset = ApiConversions.toV1(participant1x.ledger_api_v2.state.end())
    // Generate some data after the pruning point
    participant1x.health.ping(participant1x)

    def assertCannotPrune(task: => Unit, clue: String): Unit = withClue(clue) {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        task,
        logentry =>
          logentry.warningMessage should include(
            "Canton participant pruning not supported in canton-open-source edition"
          ),
        logentry => logentry.errorMessage should include(PruningNotSupportedInCommunityEdition.id),
      )
    }

    assertCannotPrune(participant1x.pruning.prune(startOffset), "prune")
    assertCannotPrune(participant1x.pruning.prune_internally(startOffset), "prune_internally")

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1x.pruning.find_safe_offset(),
      // TODO(#5990) find_safe_offset uses sync inspection and doesn't go through a gRPC error with an error code
      logentry =>
        logentry.errorMessage should include(PruningNotSupportedInCommunityEdition.Error().cause),
    )
  }
}

final class EnterpriseFeatureInCommunityReferenceXIntegrationTest
    extends EnterpriseFeatureInCommunityXIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[CommunityStorageConfig.Memory](loggerFactory)
  )
}
