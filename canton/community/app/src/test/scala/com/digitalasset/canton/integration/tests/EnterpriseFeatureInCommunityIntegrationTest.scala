// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.console.{CommandFailure, LocalDomainReference}
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  CommunityTestConsoleEnvironment,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.PruningNotSupportedInCommunityEdition
import monocle.macros.syntax.lens.*

class EnterpriseFeatureInCommunityIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {
  override def environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopology
      .addConfigTransforms(
        _.focus(_.features.enableTestingCommands).replace(true), // For ping
        _.focus(_.features.enablePreviewCommands).replace(true), // For pruning
        CommunityConfigTransforms.uniquePorts,
      )
      .withSetup { implicit env =>
        // we're only testing domain commands immediately so only start that
        mydomain.start()
      }

  "sequencer and mediator enterprise admin commands should gracefully fail" in { implicit env =>
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      mydomain.sequencer.pruning.prune(),
      // logged at the server
      _.warningMessage should include(
        "This Community edition of canton does not support the operation: EnterpriseSequencerAdministrationService.Prune."
      ),
      // logged at the client
      _.commandFailureMessage should include("unsupported by the Community edition of canton"),
    )

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      mydomain.mediator.prune(),
      // logged at the server
      _.warningMessage should include(
        "This Community edition of canton does not support the operation: EnterpriseMediatorAdministrationService.Prune."
      ),
      // logged at the client
      _.commandFailureMessage should include("unsupported by the Community edition of canton"),
    )
  }

  "participant pruning should fail gracefully" in { implicit env =>
    import env.*

    participant1.start()
    participant1.domains.connect_local(mydomain)

    val startOffset = participant1.ledger_api.completions.end()
    // Generate some data after the pruning point
    participant1.health.ping(participant1)

    def assertCannotPrune(task: => Unit, clue: String): Unit = withClue(clue) {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        task,
        _.warningMessage should include(
          "Canton participant pruning not supported in canton-open-source edition"
        ),
        _.errorMessage should include(PruningNotSupportedInCommunityEdition.id),
      )
    }

    assertCannotPrune(participant1.pruning.prune(startOffset), "prune")
    assertCannotPrune(participant1.pruning.prune(startOffset), "prune_internally")

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.pruning.find_safe_offset(),
      // TODO(#5990) find_safe_offset uses sync inspection and doesn't go through a gRPC error with an error code
      _.errorMessage should include(PruningNotSupportedInCommunityEdition.Error().cause),
    )
  }

  private def mydomain(implicit env: CommunityTestConsoleEnvironment): LocalDomainReference =
    env.d("mydomain")
}
