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

        sequencer1.start()
        mediator1.start()

        sequencer1.health.status shouldBe NodeStatus.NotInitialized(true)
        mediator1.health.status shouldBe NodeStatus.NotInitialized(true)

        bootstrap.domain(
          domainAlias,
          Seq(sequencer1),
          Seq(mediator1),
          Seq[InstanceReference](sequencer1, mediator1),
        )

        sequencer1.health.wait_for_initialized()
        mediator1.health.wait_for_initialized()

        sequencer1.health.status shouldBe a[NodeStatus.Success[?]]
        mediator1.health.status shouldBe a[NodeStatus.Success[?]]
      }

  "sequencer and mediator enterprise admin commands should gracefully fail" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      sequencer1.pruning.prune(),
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
      mediator1.pruning.prune(),
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

    participant1.start()
    participant1.domains.connect_local(
      sequencer1,
      alias = DomainAlias.tryCreate(domainAlias),
    )

    val startOffset = ApiConversions.toV1(participant1.ledger_api.state.end())
    // Generate some data after the pruning point
    participant1.health.ping(participant1)

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

    assertCannotPrune(participant1.pruning.prune(startOffset), "prune")
    assertCannotPrune(participant1.pruning.prune_internally(startOffset), "prune_internally")

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.pruning.find_safe_offset(),
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
