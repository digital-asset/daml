// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.Party

import java.time.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Test that an explicitly-disclosed contract created before an LSU can be used after the LSU. */
final class LSUExplicitDisclosureIntegrationTest extends LSUBase {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def testName: String = "lsu-explicit-disclosure"

  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup(implicit env => defaultEnvironmentSetup())

  "Logical synchronizer upgrade" should {
    "allow explicitly disclosed contracts created before LSU to be used after LSU" in {
      implicit env =>
        import env.*
        val fixture = fixtureWithDefaults()

        participants.all.dars.upload(CantonTestsPath)
        val alice = participant1.parties.testing.enable("Alice")
        val bob = participant1.parties.testing.enable("Bob")

        val (cid, aliceDisclosed) = createDisclosedContract(alice, participant1)

        clue("Before LSU, verify Bob can use Alice's contract via explicit disclosure") {
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(bob),
            commands =
              cid.exercisePriceQuotation_Fetch(bob.toProtoPrimitive).commands.asScala.toSeq,
            disclosedContracts = Seq(aliceDisclosed),
          )
        }

        clue("do LSU") {
          performSynchronizerNodesLSU(fixture)
          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
          eventually() {
            participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
          }
          oldSynchronizerNodes.all.stop()
          environment.simClock.value.advance(Duration.ofSeconds(1))
          waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
        }

        clue("After LSU, verify Bob can still use Alice's contract via explicit disclosure") {
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(bob),
            commands =
              cid.exercisePriceQuotation_Fetch(bob.toProtoPrimitive).commands.asScala.toSeq,
            disclosedContracts = Seq(aliceDisclosed),
          )
        }
    }
  }

  private def createDisclosedContract(
      party: Party,
      participant: LocalParticipantReference,
  ): (PriceQuotation.ContractId, DisclosedContract) = {
    val quote = new PriceQuotation(party.toProtoPrimitive, "DAML", 6865)
    val txn = participant.ledger_api.javaapi.commands.submit(
      actAs = Seq(party),
      commands = quote.create.commands.asScala.toSeq,
      includeCreatedEventBlob = true,
    )
    val contract = JavaDecodeUtil.decodeAllCreated(PriceQuotation.COMPANION)(txn).loneElement
    val disclosedContract = JavaDecodeUtil.decodeDisclosedContracts(txn).loneElement
    (contract.id, disclosedContract)
  }
}
