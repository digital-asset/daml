// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.File
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.tests.upgrades
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.RepresentativePackageIdOverride
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, LfPackageName, protocol}
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

class AcsImportRepresentativePackageIdSelectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with RepairTestUtil {

  private val FooV1PkgId = upgrades.v1.java.foo.Foo.PACKAGE_ID
  private val FooV2PkgId = upgrades.v2.java.foo.Foo.PACKAGE_ID
  private var alice: PartyId = _
  private var contractId: LfContractId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(FooV1Path)
        alice = participant1.parties.enable("Alice", synchronizer = daName)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  "Importing an ACS" should {
    // This test case uses participant 2 as import target
    "preserve the original package-id as representative package-id if no override" in {
      implicit env =>
        import env.*

        // Upload Foo V1 to P2
        participant2.dars.upload(FooV1Path)

        // Create a contract on P1
        contractId = createContract(participant1, alice)

        // Check the initial rp-id of the contract is the same as the original template-id
        rpIdOfContract(contractId, participant1) shouldBe FooV1PkgId

        // Export on P1 and import on P2 without any override
        exportAndImportOn(participant2)

        // Check the rp-id of contract after import on P2 is unchanged
        rpIdOfContract(contractId, participant2) shouldBe FooV1PkgId
    }

    // Tests below use participant 3 as import target
    "fail on unknown package-name for the imported contract" in { implicit env =>
      import env.*

      // Note: here we re-use the contract created in the previous test case
      //       to avoid ambiguity on ACS export/import contract order
      //       and to have a stable contractId for logging assertion below

      exportAndImportOn(
        importParticipant = participant3,
        handleImport = importAcs =>
          assertThrowsAndLogsCommandFailures(
            importAcs(),
            entry => {
              entry.shouldBeCantonErrorCode(ImportAcsError)
              entry.message should include(
                show"Could not select a representative package-id for contract with id $contractId. No package in store for the contract's package-name 'foo'"
              )
            },
          ),
      )
    }

    "select a known package for a contract if the original rp-id is not known" in { implicit env =>
      import env.*

      // Upload only Foo V2 to P3
      participant3.dars.upload(FooV2Path, vetAllPackages = false)

      // Create a contract on P1
      // P1 has only Foo V1
      val contractId = createContract(participant1, alice)

      // Export on P1 and import on P2 without any override
      exportAndImportOn(participant3)

      // Check the rp-id of contract after import on P3 (Foo V2)
      rpIdOfContract(contractId, participant3) shouldBe FooV2PkgId
    }

    "consider overrides" in { implicit env =>
      import env.*

      // Both participants have both versions of Foo
      participant1.dars.upload(FooV2Path)
      participant3.dars.upload(FooV1Path)
      // Create two contracts on P1
      val contractId1 = createContract(participant1, alice)
      val contractId2 = createContract(participant1, alice)

      // Check the initial rp-id of the contract is the same as the original template-id (Foo V2)
      rpIdOfContract(contractId1, participant1) shouldBe FooV2PkgId
      rpIdOfContract(contractId2, participant1) shouldBe FooV2PkgId

      // Export on P1 and import on P2 without any override
      exportAndImportOn(
        participant3,
        contractOverride = Map(
          contractId1 -> FooV1PkgId,
          contractId2 -> LfPackageId.assertFromString("unknown-pkg-id"),
        ),
      )

      // Representative package ID selection for contract 1 should have considered Foo V1 since it's known to P3
      rpIdOfContract(contractId1, participant3) shouldBe FooV1PkgId
      // Override for contract 2 should be ignored since the package-id is unknown to P3
      rpIdOfContract(contractId2, participant3) shouldBe FooV2PkgId
    }

    // TODO(#28075): Test vetting-based override when implemented
  }

  private def rpIdOfContract(
      contractId: LfContractId,
      participantRef: ParticipantReference,
  ): String =
    participantRef.ledger_api.state.acs
      // TODO(#27872): Re-enable verbose mode once we can verbose-render using the rp-id
      .of_party(alice, verbose = false)
      .find(_.contractId == contractId.coid)
      .value
      .event
      .representativePackageId

  private def exportAndImportOn(
      importParticipant: ParticipantReference,
      contractOverride: Map[ContractId, LfPackageId] = Map.empty,
      packageIdOverride: Map[LfPackageId, LfPackageId] = Map.empty,
      packageNameOverride: Map[LfPackageName, LfPackageId] = Map.empty,
      handleImport: (() => Unit) => Unit = (f: () => Unit) => f(),
  )(implicit env: FixtureParam): Unit = {
    import env.*

    File.usingTemporaryFile() { file =>
      participant1.repair.export_acs(
        parties = Set(alice),
        exportFilePath = file.canonicalPath,
        synchronizerId = Some(daId),
        ledgerOffset = NonNegativeLong.tryCreate(participant1.ledger_api.state.end()),
      )

      importParticipant.synchronizers.disconnect_all()
      handleImport { () =>
        try {
          importParticipant.repair
            .import_acs(
              importFilePath = file.canonicalPath,
              representativePackageIdOverride = RepresentativePackageIdOverride(
                contractOverride = contractOverride,
                packageIdOverride = packageIdOverride,
                packageNameOverride = packageNameOverride,
              ),
            )
            .discard
        } finally {
          importParticipant.synchronizers.reconnect_all()
        }
      }
    }
  }

  private def createContract(participantRef: ParticipantReference, party: PartyId)(implicit
      env: FixtureParam
  ): protocol.LfContractId = {
    import env.*

    participantRef.ledger_api.javaapi.commands
      .submit(
        Seq(party.toLf),
        new upgrades.v1.java.foo.Foo(party.toProtoPrimitive).create().commands().asScala.toSeq,
      )
      .getEvents
      .asScala
      .loneElement
      .getContractId
      .pipe(LfContractId.assertFromString)
  }
}
