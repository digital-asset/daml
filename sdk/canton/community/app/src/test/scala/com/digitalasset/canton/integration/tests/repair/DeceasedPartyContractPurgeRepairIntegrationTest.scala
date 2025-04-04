// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId

import java.util.concurrent.atomic.AtomicReference

trait DeceasedPartyContractPurgeRepairIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasExecutionContext
    with RepairTestUtil {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair)
      )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val iouBobOwnedByAlice = new AtomicReference[Option[LfContractId]](None)
  private val iouAliceOwnedByBob = new AtomicReference[Option[LfContractId]](None)
  private val iouCarolOwnedByAlice =
    new AtomicReference[Option[iou.Iou.ContractId]](None)
  private val iouAliceOwnedByCarol =
    new AtomicReference[Option[iou.Iou.ContractId]](None)

  "Able to set up participants 1, 2, and 3 with data" in { implicit env =>
    import env.*
    Seq(participant1, participant2, participant3).map { p =>
      p.synchronizers.connect_local(sequencer1, alias = daName)
      p.dars.upload(CantonExamplesPath)
    }

    val alice = participant1.parties.enable(
      aliceS,
      synchronizeParticipants = Seq(participant2, participant3),
    )
    val bob = participant2.parties.enable(
      bobS,
      synchronizeParticipants = Seq(participant1, participant3),
    )
    val carol = participant3.parties.enable(
      carolS,
      synchronizeParticipants = Seq(participant1, participant2),
    )

    iouAliceOwnedByBob.set(Some(createContract(participant1, alice, bob).toLf))
    iouBobOwnedByAlice.set(Some(createContract(participant2, bob, alice).toLf))
    iouAliceOwnedByCarol.set(Some(createContract(participant1, alice, carol)))
    iouCarolOwnedByAlice.set(Some(createContract(participant3, carol, alice)))

    assertAcsCounts(
      (participant1, Map(alice -> 4, bob -> 2, carol -> 2)),
      (participant2, Map(alice -> 2, bob -> 2, carol -> 0)),
      (participant3, Map(alice -> 2, bob -> 0, carol -> 2)),
    )
  }

  "After Bob's tragic passing away Alice cleans up her worthless contracts via repair.purge" in {
    implicit env =>
      import env.*

      // Take down Bob's participant
      participant2.stop()

      eventually() {
        participant2.is_running shouldBe false
      }

      // Alice purges her contracts with Bob:
      participant1.synchronizers.disconnect(daName)

      participant1.repair
        .purge(
          daName,
          iouBobOwnedByAlice.get.toList ++ iouAliceOwnedByBob.get.toList,
          ignoreAlreadyPurged = false,
        )
      participant1.repair
        .purge(
          daName,
          iouBobOwnedByAlice.get.toList ++ iouAliceOwnedByBob.get.toList,
          ignoreAlreadyPurged = true,
        )

      participant1.synchronizers.reconnect_all()

      val alice = aliceS.toPartyId(participant1)
      val carol = carolS.toPartyId(participant3)

      assertAcsCounts(
        (participant1, Map(alice -> 2, carol -> 2)),
        (participant3, Map(alice -> 2, carol -> 2)),
      )

      // alice and carol should still be able to exercise their IOUs
      exerciseContract(participant1, alice, iouCarolOwnedByAlice.get.value)
      exerciseContract(participant3, carol, iouAliceOwnedByCarol.get.value)

      assertAcsCountsWithFilter(
        _.templateId.isModuleEntity("Iou", "Iou"),
        (participant1, Map(alice -> 0, carol -> 0)),
        (participant3, Map(alice -> 0, carol -> 0)),
      )

      assertAcsCountsWithFilter(
        _.templateId.isModuleEntity("Iou", "GetCash"),
        (participant1, Map(alice -> 2, carol -> 2)),
        (participant3, Map(alice -> 2, carol -> 2)),
      )
  }

}

class DeceasedPartyContractPurgeRepairIntegrationTestPostgres
    extends DeceasedPartyContractPurgeRepairIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class DeceasedPartyContractPurgeRepairBftOrderingIntegrationTestPostgres
    extends DeceasedPartyContractPurgeRepairIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
