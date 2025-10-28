// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ConsoleCommandTimeout, DbConfig}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.sequencer.channel.SequencerChannelProtocolTestExecHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationSourceParticipantProcessor,
  PartyReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.nowarn
import scala.concurrent.Future

/** Objectives:
  *   - Test the behavior of the [[PartyReplicationSourceParticipantProcessor]] and
  *     [[PartyReplicationTargetParticipantProcessor]] in replicating a party's active contracts via
  *     the online party replication participant protocol from a source participant SP to a target
  *     participant TP.
  *   - Test that TP-side conflict detection properly handles concurrent contract archives and
  *     unassignments of contracts before they are replicated to the target participant.
  *
  * Setup:
  *   - 2 participants, a source and a target participant to perform party replication
  *   - 2 synchronizers with 1 sequencer/mediator each: the "da" synchronizer is used for party
  *     replication while the "acme" synchronizer only exists as a target for an unassignment of a
  *     contract during party replication.
  */
@nowarn("msg=match may not be exhaustive")
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
sealed trait OnlinePartyReplicationParticipantProtocolTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SequencerChannelProtocolTestExecHelpers
    with HasCycleUtils {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  private val aliceName = "Alice"

  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.unsafeEnableOnlinePartyReplication()*
      )
      .withSetup { implicit env =>
        import env.*
        // More frequent ACS commitments by configuring a smaller reconciliation interval.
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(10)),
        )

        participants.local.start()
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = Some(daName),
        )
        participant1.synchronizers.connect_local(sequencer2, acmeName)
        participant1.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        participant1.parties.enable(aliceName, synchronizer = Some(acmeName)).discard
      }

  // OnlinePartyReplication messages require dev protocol version for added safety
  // to ensure that they aren't accidentally used in production.
  "Replicate a party's active contracts from a source to a target participant" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      // Create a handful of contracts on the source participant
      val numContracts = 120
      createCycleContracts(sourceParticipant, alice, (1 to numContracts).map(i => s"cycle $i"))
      logger.info(s"Created $numContracts contracts on the source participant")

      // Remember two of the contracts near the "end" so we can deactivate them later during ACS replication
      val (nThLastCycleIndexForArchival, nThLastCycleIndexForUnassign) = (2, 3)
      require(
        numContracts > nThLastCycleIndexForArchival && numContracts > nThLastCycleIndexForUnassign
      )
      val cycleToArchive = sourceParticipant.ledger_api.javaapi.state.acs
        .await(M.Cycle.COMPANION)(
          alice,
          cycle => cycle.data.id == s"cycle ${numContracts - nThLastCycleIndexForArchival}",
        )
      val cycleToUnassign = sourceParticipant.ledger_api.javaapi.state.acs
        .await(M.Cycle.COMPANION)(
          alice,
          cycle => cycle.data.id == s"cycle ${numContracts - nThLastCycleIndexForUnassign}",
        )

      // Create one cycle contract on the target participant owner by the admin so that the TP vets the cycle packages
      createCycleContract(targetParticipant, targetParticipant.adminParty, "admin cycle")

      // Host Alice as an observer on the target participant to represent the authorization to replicate the party.
      // This mainly serves to provide a timestamp for the ACS snapshot.
      Seq(sourceParticipant, targetParticipant).foreach { participant =>
        participant.topology.party_to_participant_mappings
          .propose_delta(
            alice,
            adds = Seq((targetParticipant.id, ParticipantPermission.Observation)),
            requiresPartyToBeOnboarded = true,
            store = daId,
            mustFullyAuthorize = participant == targetParticipant,
          )
          .discard
      }
      // Wait for partyToParticipant mapping to become effective on the target participant
      val partyToTargetParticipantEffectiveAt = eventually() {
        val p2p = targetParticipant.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.filterString)
          .loneElement

        p2p.item.participants.map(_.participantId) should contain theSameElementsAs Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )

        CantonTimestamp.assertFromInstant(p2p.context.validFrom)
      }

      // Wait for partyToParticipant mapping to become effective on the source participant via ledger api
      val partyToSourceParticipantEffectiveAtOffset = eventually() {
        val offsetPartyAddedToTPO = (for {
          ptp <- sourceParticipant.ledger_api.updates.topology_transactions(
            completeAfter = PositiveInt.two,
            partyIds = Seq(alice),
            synchronizerFilter = Some(daId),
          )
          topologyEvent <- ptp.topologyTransaction.events
          offset = ptp.topologyTransaction.offset
          partyAdded <- topologyEvent.event.participantAuthorizationAdded.toList
        } yield (partyAdded, offset)).collect {
          case (partyAdded, offset)
              if partyAdded.participantId == targetParticipant.id.uid.toProtoPrimitive =>
            Offset.tryFromLong(offset)
        }.lastOption
        offsetPartyAddedToTPO.nonEmpty shouldBe true
        offsetPartyAddedToTPO.value
      }

      // Run a ping to make sure the AcsInspection does not get upset about the timestamp at the end
      // of the event log.
      sourceParticipant.health.ping(sourceParticipant)

      val Seq(spClient, tpClient) = Seq(sourceParticipant, targetParticipant).map(
        _.testing.state_inspection.getSequencerChannelClient(daId).value
      )

      asyncExec("ping1")(spClient.ping(sequencer1.id))
      asyncExec("ping2")(tpClient.ping(sequencer1.id))

      val channelId = SequencerChannelId("channel1")
      val requestId = TestHash.build.add("OnlinePartyReplicationParticipantProtocolTest").finish()

      def noOpProgressAndCompletionCallback[T]: T => Unit = _ => ()
      def noOpProgressAndCompletionCallback2[T, U]: (T, U) => Unit = (_, _) => ()

      val sourceProcessor = PartyReplicationSourceParticipantProcessor(
        daId,
        alice,
        requestId,
        partyToSourceParticipantEffectiveAtOffset,
        Set.empty,
        sourceParticipant.underlying.value.sync.internalIndexService.value,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback2,
        futureSupervisor,
        exitOnFatalFailures = true,
        timeouts,
        loggerFactory,
      )
      val connectedSynchronizer =
        targetParticipant.underlying.value.sync.connectedSynchronizerForAlias(daName).value

      // This flag, when set to true will unblock the target participant processor.
      var canTargetParticipantProceed: Boolean = false
      val handfulOfContractsCountToProcessBeforePausing = 4
      val targetProcessor = PartyReplicationTargetParticipantProcessor(
        alice,
        requestId,
        partyToTargetParticipantEffectiveAt,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback2,
        targetParticipant.underlying.value.sync.participantNodePersistentState,
        connectedSynchronizer,
        futureSupervisor,
        exitOnFatalFailures = true,
        timeouts,
        loggerFactory,
        PartyReplicationTestInterceptorImpl
          .targetParticipantProceedsIf(
            // wait for a handful of contracts
            canTargetParticipantProceed || _.processedContractsCount.unwrap < handfulOfContractsCountToProcessBeforePausing
          ),
      )

      val sessionKeyOwner = true
      val noSessionKeyOwner = false
      // Connect in parallel as it does not matter which participant connects first.
      asyncExec("Connect source and target participants")(
        Seq(
          ("source", spClient, sourceProcessor, participant2, noSessionKeyOwner),
          ("target", tpClient, targetProcessor, participant1, sessionKeyOwner),
        )
          .parTraverse_ { case (role, client, processor, recipientParticipant, keyOwner) =>
            clue(s"connect $role participant to sequencer channel")(
              client.connectToSequencerChannel(
                sequencer1.id,
                channelId,
                recipientParticipant.id,
                processor,
                isSessionKeyOwner = keyOwner,
                partyToTargetParticipantEffectiveAt,
              )
            ).mapK(FutureUnlessShutdown.liftK)
          }
      )

      eventually() {
        sourceProcessor.isChannelConnected shouldBe true
        targetProcessor.isChannelConnected shouldBe true
      }

      Future {
        // Have the test play the role of the PartyReplicator and drive the source processor.
        // This is necessary since reading the ACS via pekko is asynchronous and thus the SP
        // processor needs to regularly check if a partially available ACS batch is not fully
        // available for responding to the TP.
        clue("Regularly ensure the SP does not get stuck")(
          while (!sourceProcessor.hasChannelCompleted) {
            sourceProcessor.progressPartyReplication()
            Threading.sleep(1000)
          }
        )
      }.discard

      // Check that archiving a contract that is still unknown on the TP
      // does not cause problem with the conflict detection activeness check
      // such as the alarm: "Request with failed activeness check is approved."
      clue("archiving one of the contracts in the replicating ACS") {
        sourceParticipant.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(cycleToArchive.id.exerciseArchive().commands.loneElement),
          optTimeout = None, // don't wait for response from TP where indexing is paused
        )
      }

      // Check that unassigning a contract that does not yet exist on the TP
      // does not cause problem with the conflict detection activeness check.
      clue("unassigning one of the contracts in the replicating ACS") {
        participant1.ledger_api.commands
          .submit_unassign(
            alice,
            Seq(cycleToUnassign.id.toLf),
            daId,
            acmeId,
            timeout = None, // don't wait for response from TP
          )
          .reassignmentId
      }

      // Create a few contracts to buffer and flush without timeout as the TP will not
      // witness the events until after OnPR completes.
      val lastContractIndex = 104
      (101 until lastContractIndex).foreach { i =>
        clue(s"Creating contract $i")(
          createCycleContract(sourceParticipant, alice, s"cycle $i", optTimeout = None)
        )
      }

      // Set the flag and wake up the target participant processor to resume OnPR.
      logger.info("Unblocking the target participant processor")
      canTargetParticipantProceed = true
      targetProcessor.progressPartyReplication()

      clue(s"Creating contract $lastContractIndex")(
        createCycleContract(
          sourceParticipant,
          alice,
          s"cycle $lastContractIndex",
          optTimeout = Some(ConsoleCommandTimeout.defaultLedgerCommandsTimeout),
        )
      )

      eventually() {
        sourceProcessor.hasChannelCompleted shouldBe true
        targetProcessor.hasChannelCompleted shouldBe true
      }

      logger.info("Channel completed on SP and TP")

      logger.info("Check that the contracts are now visible on TP via the Ledger API")
      val sourceContractIds = sourceParticipant.ledger_api.state.acs
        .of_party(
          alice,
          // don't count the IncompleteUnassigned contract
          resultFilter = _.contractEntry.activeContract.nonEmpty,
        )
        .map(_.contractId)
        .toSet
      eventually() {
        val targetContractIds = targetParticipant.ledger_api.state.acs
          .of_party(alice)
          .map(_.contractId)
          .toSet

        logger.info(s"sourceContractIds: $sourceContractIds")
        logger.info(s"targetContractIds: $targetContractIds")
        targetContractIds should not be empty
        targetContractIds.size shouldBe sourceContractIds.size
        targetContractIds shouldBe sourceContractIds
      }

      logger.info("Check that the contracts are now visible on TP in the canton-internal stores")
      val targetCantonContracts = targetParticipant.testing.state_inspection
        .findContracts(
          daName,
          None,
          None,
          filterTemplate = Some("^Cycle:Cycle"),
          limit = numContracts + 2 + 4, // +2 for deactivations and +4 for creates above
        )
        .collect {
          case (true, contract) if contract.metadata.stakeholders.contains(alice.toLf) => contract
        }
      val excessContracts =
        targetCantonContracts.collect {
          case c if !sourceContractIds.contains(c.contractId.coid) =>
            (
              c.contractId,
              c.templateId,
              c.inst.createArg,
            )
        }
      if (excessContracts.nonEmpty) {
        logger.info(s"Found unexpected excess contracts ${excessContracts.mkString("\n")}")
        logger.info(s"All contracts: \n${targetCantonContracts.mkString("\n")}")
      }
      excessContracts shouldBe Seq.empty
      targetCantonContracts.size shouldBe sourceContractIds.size
  }
}

final class OnlinePartyReplicationParticipantProtocolTestPostgres
    extends OnlinePartyReplicationParticipantProtocolTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
