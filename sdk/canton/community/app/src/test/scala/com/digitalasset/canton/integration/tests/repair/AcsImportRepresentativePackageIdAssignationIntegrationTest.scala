// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.File
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.RepresentativePackageIdOverride
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId

import scala.util.chaining.scalaUtilChainingOps

class AcsImportRepresentativePackageIdAssignationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with RepairTestUtil {

  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
        alice = participant1.parties.enable("Alice", synchronizer = daName)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  // TODO(#27872): Complete test coverage for representative package ID overrides
  "Importing an ACS" should {
    "correctly assign the representative package-id with contract override" in { implicit env =>
      import env.*

      val contractId1 = IouSyntax
        .createIou(participant1)(alice, alice)
        .id
        .contractId
        .pipe(LfContractId.assertFromString)

      val contractId2 = IouSyntax
        .createIou(participant1)(alice, alice)
        .id
        .contractId
        .pipe(LfContractId.assertFromString)

      def rpIdOfContract(contractId: LfContractId, participantRef: ParticipantReference): String =
        participantRef.ledger_api.state.acs
          .of_party(alice)
          .find(_.contractId == contractId.coid)
          .value
          .event
          .representativePackageId

      // Check the initial rp-id of the contracts is the same as the original tempalate-id
      rpIdOfContract(contractId1, participant1) shouldBe IouSyntax.modelCompanion.PACKAGE_ID
      rpIdOfContract(contractId2, participant1) shouldBe IouSyntax.modelCompanion.PACKAGE_ID

      File.usingTemporaryFile() { file =>
        participant1.repair.export_acs(
          parties = Set(alice),
          exportFilePath = file.canonicalPath,
          synchronizerId = Some(daId),
          ledgerOffset = NonNegativeLong.tryCreate(participant1.ledger_api.state.end()),
        )

        val newRpIdForContract1 = "some-rp-id"
        participant2.synchronizers.disconnect_all()
        participant2.repair.import_acs(
          importFilePath = file.canonicalPath,
          representativePackageIdOverride = RepresentativePackageIdOverride(
            contractOverride =
              Map(contractId1 -> LfPackageId.assertFromString(newRpIdForContract1)),
            packageIdOverride = Map.empty,
            packageNameOverride = Map.empty,
          ),
        )
        participant2.synchronizers.reconnect_all()

        // Check the rp-id of contract 1 after import on p3 has been updated according to the contract override mapping
        rpIdOfContract(contractId1, participant2) shouldBe newRpIdForContract1
        // Check the rp-id of contract 2 is unchanged after import since it was not in the contract override mapping
        rpIdOfContract(contractId2, participant2) shouldBe IouSyntax.modelCompanion.PACKAGE_ID
      }
    }
  }
}
