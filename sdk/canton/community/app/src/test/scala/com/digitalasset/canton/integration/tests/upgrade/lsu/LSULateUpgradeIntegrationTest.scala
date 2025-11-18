// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import better.files.File
import com.daml.ledger.api.v2.topology_transaction.TopologyEvent.Event
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId

/*
 * This test is used to test manual late upgrade.
 *
 * Topology:
 * - Bank hosted on P1
 * - Alice hosted on P2
 * - Charlie hosted on P3
 *
 * Upgrade:
 * - P1: on time (regular LSU)
 * - P2: late (misses the announcement and the upgrade time)
 * - P3: late (sees the announcement but is offline during upgrade)
 *
 * Scenario:
 * - Some activity happens involving P1, P2 and P3
 * - P2 disconnects
 * - LSU is announced
 * - P3 disconnects
 * - LSU happens
 * - Old synchronizer is decommissioned
 * - P2 and P3 do the manual upgrade and manual repair of ACS
 *
 * Goals:
 * - Check the late upgrade can be done
 * - Check that ACS upgrade can be done
 * - Check that topology is correct at the end
 * - Check that PTP updates are correct at the end
 *
 * Other participants:
 * - P4 only registers the old synchronizer but neber connects to it
 */
abstract class LSULateUpgradeIntegrationTest extends LSUBase {

  override protected def testName: String = "logical-synchronizer-upgrade"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  // parties
  private var bank: PartyId = _
  private var alice: PartyId = _
  private var charlie: PartyId = _
  private var lateParty1: PartyId = _

  private var fixture: LSUBase.Fixture = _

  // Missed IOUs because participants are offline
  private var aliceMissedIou: LfContractId = _
  private var charlieMissedIou: LfContractId = _

  "Manual late upgrade" should {
    // Create parties and contracts, LSU is performed for P1
    "creating scenario" in { implicit env =>
      import env.*

      fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      bank = participant1.parties.enable("Bank")
      alice = participant2.parties.enable("Alice")
      charlie = participant3.parties.enable("Charlie")

      IouSyntax.createIou(participant1)(bank, alice, amount = 10.0)
      IouSyntax.createIou(participant1)(bank, charlie, amount = 20.0)

      // Only registers the synchronizer
      participant4.synchronizers.register(sequencer1, daName, performHandshake = false)

      participant2.synchronizers.disconnect_all()

      // P2, P3, P4 miss that allocation
      lateParty1 = participant1.parties.enable("LateParty1")

      // P2 will miss that create
      aliceMissedIou = IouSyntax
        .createIou(participant1)(
          bank,
          alice,
          amount = 21.0,
          // participant2 is offline
          optTimeout = None,
        )
        .id
        .contractId
        .toLfContractId

      performSynchronizerNodesLSU(fixture)

      // P3 performs handshake with the new synchronizer
      eventually() {
        participant3.underlying.value.sync.syncPersistentStateManager
          .get(fixture.newPSId)
          .value
          .parameterStore
          .lastParameters
          .futureValueUS should not be empty
      }

      // P3 disconnects before upgrade time is reached
      participant3.synchronizers.disconnect_all()

      // P3 will miss that one
      charlieMissedIou = IouSyntax
        .createIou(participant1)(
          bank,
          charlie,
          amount = 21.0,
          // participant3 is offline
          optTimeout = None,
        )
        .id
        .contractId
        .toLfContractId

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true
      }

      sequencer1.stop()
      mediator1.stop()

      // P2 and P3 misses that allocation on the new synchronizer
      env.participant1.parties.enable("LateParty2")

      // Create more contracts. Those don't need repair as they are created on the new synchronizer
      IouSyntax
        .createIou(participant1)(
          bank,
          alice,
          amount = 12.0,
          // participant2 is offline
          optTimeout = None,
        )

      IouSyntax
        .createIou(participant1)(
          bank,
          charlie,
          amount = 22.0,
          // participant2 is offline
          optTimeout = None,
        )

    }

    "late upgrades" in { implicit env =>
      import env.*

      val newSynchronizerConnectionConfig =
        participant1.synchronizers.config(fixture.newPSId).value

      def manualLSU(p: ParticipantReference): Unit = p.repair.perform_synchronizer_upgrade(
        currentPhysicalSynchronizerId = daId,
        successorPhysicalSynchronizerId = fixture.newPSId,
        announcedUpgradeTime = fixture.upgradeTime,
        successorConfig = newSynchronizerConnectionConfig,
      )

      /** Late upgrade of participant p
        * @param p
        *   Participant that need to upgrade
        * @param cid
        *   Contract to export
        * @param upgradeBeforeACSRepair
        *   Whether the manual LSU is done before the repair of the ACS
        */
      def lateUpgrade(
          p: ParticipantReference,
          cid: LfContractId,
          upgradeBeforeACSRepair: Boolean,
      ): Unit = {
        if (upgradeBeforeACSRepair) {
          manualLSU(p)
          manualLSU(p) // idempotency
        }

        // Repair the ACS by importing missing contracts
        val acsFile: String = File.newTemporaryFile(s"late-lsu-${p.name}").canonicalPath

        val contracts = env.participant1.ledger_api.state.acs
          .active_contracts_of_party(
            bank,
            includeCreatedEventBlob = true,
            filterTemplates = Seq(TemplateId.fromJavaIdentifier(Iou.TEMPLATE_ID)),
          )
          .filter(_.getCreatedEvent.contractId == cid.coid)

        participant1.repair.write_contracts_to_file(
          contracts,
          testedProtocolVersion,
          acsFile,
        )

        p.repair.import_acs(acsFile)

        if (!upgradeBeforeACSRepair) {
          manualLSU(p)
          manualLSU(p) // idempotency
        } else {
          manualLSU(p) // idempotency for the other case again
        }

        p.synchronizers.reconnect_all()
        eventually() {
          p.synchronizers.is_connected(fixture.newPSId) shouldBe true
        }

        p.health.ping(env.participant1.id)
      }

      lateUpgrade(env.participant2, aliceMissedIou, upgradeBeforeACSRepair = true)
      lateUpgrade(env.participant3, charlieMissedIou, upgradeBeforeACSRepair = false)

      // P4 late upgrade
      manualLSU(participant4)
      eventually() {
        participant4.synchronizers.is_connected(fixture.newPSId) shouldBe true
      }

      participant4.health.ping(env.participant1.id)
    }

    "checking topology" in { implicit env =>
      import env.*

      val allParties = env.participant1.parties.list().map(_.party)

      // 4 admin parties, (Bank, Alice, Charlie), LateParty1, LateParty2
      val expectedPartiesCount = 4 + 3 + 2
      allParties should have size expectedPartiesCount.toLong

      def checkTopology(p: ParticipantReference, missedParties: Set[PartyId]) = {
        // Check that p sees topology changes that happened on old and new synchronizer when it was offline
        p.parties.list().map(_.party) should contain theSameElementsAs allParties

        val ledgerEnd = p.ledger_api.state.end()
        val allTopologyEventsOnP = p.ledger_api.updates
          .topology_transactions(
            PositiveInt.tryCreate(expectedPartiesCount - missedParties.size),
            endOffsetInclusive = Some(ledgerEnd),
          )
          .flatMap(_.topologyTransaction.events.map(_.event))

        val allPartiesAddedOnP = allTopologyEventsOnP.collect {
          case Event.ParticipantAuthorizationAdded(partyAdded) =>
            PartyId.tryFromProtoPrimitive(partyAdded.partyId)
        }

        // We don't want to deal with removals or changes for now
        // This is a sanity check that it was not introduced in the test by accident
        allTopologyEventsOnP should have size allPartiesAddedOnP.size.toLong

        val expectedParties = allParties.toSet -- missedParties

        allPartiesAddedOnP should contain theSameElementsAs expectedParties
      }

      //  TODO(#29087) : Allocation of lateParty1 on the old synchronizer is missed by P2. Fix it.
      checkTopology(participant2, Set(lateParty1))

      checkTopology(participant3, Set())
    }

    "check ACS" in { implicit env =>
      import env.*

      participant2.ledger_api.state.acs.active_contracts_of_party(alice) should have size 3
      participant3.ledger_api.state.acs.active_contracts_of_party(charlie) should have size 3
    }
  }
}

final class LSULateUpgradeReferenceIntegrationTest extends LSULateUpgradeIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
