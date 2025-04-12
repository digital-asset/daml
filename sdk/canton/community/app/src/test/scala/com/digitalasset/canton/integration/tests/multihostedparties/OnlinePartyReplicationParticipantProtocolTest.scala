// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, DbConfig}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
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

/** Objective: Test the behavior of the [[PartyReplicationSourceParticipantProcessor]] and
  * [[PartyReplicationTargetParticipantProcessor]] in replicating a party's active contracts via the
  * online party replication participant protocol. Setup: 2 participants, a source and a target
  * participant to perform party replication 1 sequencer/mediator
  */
@nowarn("msg=match may not be exhaustive")
sealed trait OnlinePartyReplicationParticipantProtocolTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SequencerChannelProtocolTestExecHelpers
    with HasCycleUtils {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val aliceName = "Alice"

  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
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
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            synchronizerAlias = daName,
            synchronizerOwners = Seq[InstanceReference](mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        // Before performing OPR, disable ACS commitments by having a large reconciliation interval
        // because the target participant does not have the onboarding party's active contracts.
        // TODO(#23670): Proper ACS commitment checking during/after OPR
        mediator1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )

        participants.local.foreach(_.start())
        participants.local.synchronizers.connect_local(sequencer1, daName)
        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2),
        )
      }

  // OnlinePartyReplication messages require dev protocol version for added safety
  // to ensure that they aren't accidentally used in production.
  "Replicate a party's active contracts from a source to a target participant" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      // Create a handful of contracts on the source participant
      val numContracts = 17
      (1 to numContracts).foreach { i =>
        createCycleContract(
          sourceParticipant,
          alice,
          s"cycle $i",
          // Only wait for the last contract
          optTimeout =
            Option.when(i == numContracts)(ConsoleCommandTimeout.defaultLedgerCommandsTimeout),
        )
      }
      logger.info(s"Created $numContracts contracts on the source participant")

      // Remember one of the contracts so we can archive it later during ACS replication
      val nLast = 2
      require(numContracts > nLast)
      val cycleToArchive = sourceParticipant.ledger_api.javaapi.state.acs
        .await(M.Cycle.COMPANION)(alice, cycle => cycle.data.id == s"cycle ${numContracts - nLast}")

      // Create one cycle contract on the target participant owner by the admin so that the TP vets the cycle packages
      createCycleContract(targetParticipant, targetParticipant.adminParty, "admin cycle")

      // Host Alice as an observer on the target participant to represent the authorization to replicate the party.
      // This mainly serves to provide a timestamp for the ACS snapshot.
      Seq(sourceParticipant, targetParticipant).foreach { participant =>
        participant.topology.party_to_participant_mappings
          .propose_delta(
            alice,
            adds = List((targetParticipant.id, ParticipantPermission.Observation)),
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
        .findContracts(daName, None, None, filterTemplate = Some("^Cycle:Cycle"), 100)
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
              timeouts.default.awaitUS_("Block Target Participant Processor")(
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

      // Archiving a contract that does not yet exist on the TP currently
      // triggers the alarm: "Request with failed activeness check is approved."
      // clue("archiving one of the contracts in the replicating ACS") {
      //   sourceParticipant.ledger_api.javaapi.commands.submit(
      //     Seq(alice),
      //     Seq(cycleToArchive.id.exerciseArchive().commands.loneElement),
      //   )
      // }
      cycleToArchive.discard

      // Create a few contracts to buffer and flush without timeout as the TP will not
      // witness the events until after OPR completes.
      val lastContractIndex = 104
      (101 until lastContractIndex).foreach { i =>
        clue(s"Creating contract $i")(
          createCycleContract(sourceParticipant, alice, s"cycle $i", optTimeout = None)
        )
      }
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
      replicatedContracts.flatMap(_._2).toSet shouldBe expectedContractsToReplicate.toSet
      logger.info("Sanity-check that there were no chunk counter gaps")
      replicatedContracts.map(_._1.unwrap) shouldBe replicatedContracts.indices

      logger.info("Also check that the contracts are now visible on TP via the Ledger API")
      val sourceContractIds = sourceParticipant.ledger_api.state.acs
        .of_party(alice)
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
        .findContracts(daName, None, None, filterTemplate = Some("^Cycle:Cycle"), 100)
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
