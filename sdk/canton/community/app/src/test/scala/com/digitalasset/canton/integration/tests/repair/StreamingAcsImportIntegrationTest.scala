// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.File
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission

/** This test runs an acs import for the streaming acs import endpoints in the party management
  * service and the repair service.
  *
  * We run a dedicated participant for testing the import via the party management service and
  * another one for the import via the repair service.
  */
sealed trait StreamingAcsImportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil {

  private val acsCommitmentInterval = PositiveSeconds.tryOfDays(365)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1

  "use repair to migrate a party to a different participant" in { implicit env =>
    import env.*

    val emptyAcsFile = File.newTemporaryFile()
    val acsFile = File.newTemporaryFile()

    repair.acs.write_contracts_to_file(Nil, testedProtocolVersion, emptyAcsFile.canonicalPath)

    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      _.update(reconciliationInterval = acsCommitmentInterval.toConfig),
    )

    participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    participants.all.dars.upload(CantonExamplesPath)

    val alice = participant1.parties.enable("Alice")

    val iousCount = 20
    IouSyntax.createIous(participant1, alice, alice, (1 to iousCount))

    loggerFactory.suppressWarnings {
      // Now bring Alice to participant2
      // This should trigger ACS mismatches
      Seq(participant1, participant2, participant3).foreach(
        _.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          adds = List(
            participant2.id -> ParticipantPermission.Submission,
            participant3.id -> ParticipantPermission.Submission,
          ),
          store = daId,
          requiresPartyToBeOnboarded = true,
        )
      )

      eventually() {
        Seq(participant1, participant2, participant3).foreach(p =>
          p.topology.party_to_participant_mappings.is_known(
            daId,
            alice,
            Seq(participant1, participant2, participant3),
          ) shouldBe true
        )
      }

      eventually() {
        participant1.parties.export_party_acs(
          alice,
          daId,
          targetParticipantId = participant2,
          beginOffsetExclusive = 1,
          exportFilePath = acsFile.canonicalPath,
        )
      }

      // Test acs import via the party management service
      participant2.synchronizers.disconnect(daName)

      // Test with real ACS
      participant2.parties.import_party_acsV2(acsFile.canonicalPath, daId)

      // Test with empty ACS
      participant2.parties.import_party_acsV2(emptyAcsFile.canonicalPath, daId)

      participant2.synchronizers.reconnect(daName)

      participant2.ledger_api.state.acs.of_all() should have size iousCount.toLong

      // Test acs import via the repair service
      participant3.synchronizers.disconnect(daName)

      // Test with real ACS
      participant3.repair.import_acsV2(acsFile.canonicalPath, daId)

      // Test with empty ACS
      participant3.repair.import_acsV2(emptyAcsFile.canonicalPath, daId)

      participant3.synchronizers.reconnect(daName)

      participant3.ledger_api.state.acs.of_all() should have size iousCount.toLong
    }
  }
}

final class StreamingAcsImportIntegrationTestPostgres extends StreamingAcsImportIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
}
