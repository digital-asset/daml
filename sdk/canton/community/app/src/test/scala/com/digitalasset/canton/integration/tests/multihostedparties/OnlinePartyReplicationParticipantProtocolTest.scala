// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ConsoleCommandTimeout, DbConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.sequencer.channel.SequencerChannelProtocolTestExecHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationSourceParticipantProcessor,
  PartyReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.blocking

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
sealed trait OnlinePartyReplicationParticipantProtocolTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SequencerChannelProtocolTestExecHelpers
    with HasCycleUtils {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
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
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.unsafeOnlinePartyReplication)
            .replace(
              Some(
                UnsafeOnlinePartyReplicationConfig(pauseSynchronizerIndexingDuringPartyReplication =
                  true
                )
              )
            )
        ),
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.unsafeEnableOnlinePartyReplication).replace(true)
        ),
        // TODO(#24326): While the SourceParticipant (SP=P1) uses AcsInspection to consume the
        //  ACS snapshot (rather than the Ledger Api), ensure ACS pruning does not trigger AcsInspection
        //  TimestampBeforePruning. Allow a generous 5 minutes for the SP to consume all active contracts
        //  in this test.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(config.NonNegativeFiniteDuration.ofMinutes(5))
        ),
      )
      .withSetup { implicit env =>
        import env.*
        // Before performing OPR, disable ACS commitments by having a large reconciliation interval
        // because the target participant does not have the onboarding party's active contracts.
        // TODO(#23670): Proper ACS commitment checking during/after OPR
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )

        participants.local.foreach(_.start())
        participants.local.synchronizers.connect_local(sequencer1, daName)
        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2),
          synchronizer = Some(daName),
        )
        participant1.synchronizers.connect_local(sequencer2, acmeName)
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
            adds = List((targetParticipant.id, ParticipantPermission.Observation)),
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

      val expectedContractsToReplicate = sourceParticipant.testing.state_inspection
        .findContracts(
          daName,
          None,
          None,
          filterTemplate = Some("^Cycle:Cycle"),
          limit = numContracts + 2, // +2 for the two deactivations
        )
        .collect { case (true, contract) => contract }

      // Run a ping to make sure the AcsInspection does not get upset about the timestamp at the end
      // of the event log.
      sourceParticipant.health.ping(sourceParticipant)

      val Seq(spClient, tpClient) = Seq(sourceParticipant, targetParticipant).map(
        _.testing.state_inspection.getSequencerChannelClient(daId).value
      )

      asyncExec("ping1")(spClient.ping(sequencer1.id))
      asyncExec("ping2")(tpClient.ping(sequencer1.id))

      val channelId = SequencerChannelId("channel1")

      val replicatedContracts =
        mutable.Buffer.empty[(NonNegativeInt, NonEmpty[Seq[SerializableContract]])]

      def noOpProgressAndCompletionCallback: NonNegativeInt => Unit = _ => ()

      val sourceProcessor = PartyReplicationSourceParticipantProcessor(
        daId,
        alice,
        partyToTargetParticipantEffectiveAt,
        sourceParticipant.testing.state_inspection.getAcsInspection(daId).value,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
      val connectedSynchronizer =
        targetParticipant.underlying.value.sync.connectedSynchronizerForAlias(daName).value

      // A promise blocks OnPR temporarily until the transactions have been run concurrently to OnPR.
      val promiseWhenConcurrentTransactionsSubmitted = PromiseUnlessShutdown.unsupervised[Unit]()
      val targetProcessor = PartyReplicationTargetParticipantProcessor(
        daId,
        alice,
        partyToTargetParticipantEffectiveAt,
        noOpProgressAndCompletionCallback,
        noOpProgressAndCompletionCallback,
        { (acsChunkId, contracts) =>
          replicatedContracts += ((acsChunkId, contracts))
          if (acsChunkId.unwrap == 4) {
            logger.debug(
              s"Block replication of $acsChunkId until concurrent transactions are submitted"
            )
            blocking {
              timeouts.default.awaitUS_("Block Target Participant Processor for test")(
                promiseWhenConcurrentTransactionsSubmitted.futureUS
              )
            }
            logger.debug(s"Unblocked replication of $acsChunkId")
          }
          EitherTUtil.unitUS
        },
        targetParticipant.underlying.value.sync.participantNodePersistentState,
        connectedSynchronizer,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
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
        sourceProcessor.hasChannelConnected shouldBe true
        targetProcessor.hasChannelConnected shouldBe true
      }

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
          .unassignId
      }

      // Create a few contracts to buffer and flush without timeout as the TP will not
      // witness the events until after OnPR completes.
      val lastContractIndex = 104
      (101 until lastContractIndex).foreach { i =>
        clue(s"Creating contract $i")(
          createCycleContract(sourceParticipant, alice, s"cycle $i", optTimeout = None)
        )
      }

      // Succeed the promise now that concurrent transactions have been submitted.
      promiseWhenConcurrentTransactionsSubmitted.trySuccess(UnlessShutdown.Outcome(()))

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

      logger.info(s"Check contracts were replicated in ${replicatedContracts.size} ACS chunks")
      val replicatedSet = replicatedContracts.flatMap(_._2).toSet
      expectedContractsToReplicate.toSet -- replicatedSet shouldBe Set.empty
      replicatedSet -- expectedContractsToReplicate.toSet shouldBe Set.empty
      logger.info("Sanity-check that there were no chunk counter gaps")
      replicatedContracts.map(_._1.unwrap) shouldBe replicatedContracts.indices

      logger.info("Also check that the contracts are now visible on TP via the Ledger API")
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
              c.contractInstance.unversioned.template,
              c.contractInstance.unversioned.arg,
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

class OnlinePartyReplicationParticipantProtocolTestPostgres
    extends OnlinePartyReplicationParticipantProtocolTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
