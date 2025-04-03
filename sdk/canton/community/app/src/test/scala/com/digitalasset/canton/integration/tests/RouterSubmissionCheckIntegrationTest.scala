// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission

trait RouterSubmissionCheckIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "Router should reject due missing submission rights" in { implicit env =>
    import env.*

    participants.all.synchronizers.connect_local(sequencer1, alias = daName)

    PartiesAllocator(Set(participant1, participant2))(
      newParties = Seq("Jesper" -> participant1),
      targetTopology = Map(
        "Jesper" -> Map(
          daId -> (PositiveInt.one, Set(
            participant1.id -> ParticipantPermission.Submission,
            participant2.id -> ParticipantPermission.Observation,
          ))
        )
      ),
    )
    val jesper = "Jesper".toPartyId(participant1)

    assertThrowsAndLogsCommandFailures(
      createCycleContract(participant2, jesper, id = "Behold the face of your unmaking"),
      _.commandFailureMessage should include(NoSynchronizerOnWhichAllSubmittersCanSubmit.id),
    )
  }

  // TODO(#5021): add test for multi-synchronizer scenario once idm configuration options (#1252) are implemented

}

class RouterSubmissionCheckIntegrationTestInMemory extends RouterSubmissionCheckIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))
}
