// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.admin.api.client.data.AddPartyStatus
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

/** Objective: Test party replication of non-local parties such as a decentralized party with
  * concurrent archiving of replicated contracts.
  *
  * Setup:
  *   - 3 participants: participant1 hosts a centralized party to replicate to participant2
  *   - the decentralized party's decentralized namespace is owned by separate nodes: sequencer1,
  *     mediator1, and participant3
  *   - 1 mediator/sequencer each
  */
sealed trait OnlinePartyReplicationDecentralizedPartyTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with HasProgrammableSequencer
    with SharedEnvironment {

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var alice: PartyId = _
  private var bob: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  // false means to block OnPR (temporarily) the moment the SP connects to channel
  private val numContractsInCreateBatch = 100
  private var canSourceProceedWithOnPR: Boolean = false
  private val canApproveExercise = Promise[Unit]()

  // Use the test interceptor to block OnPR until a concurrent exercise is processed by the SP
  // in such a way that the exercised contract is replicated between exercise phases 3 and 7
  // to produce a race in the TP ConflictDetector in-memory state.
  private def createSourceParticipantTestInterceptor(): PartyReplicationTestInterceptor =
    PartyReplicationTestInterceptorImpl.sourceParticipantProceedsIf { state =>
      // Allow replicating the first batch that contains the contract that this test exercises,
      // but stop before the second batch, so that we can ensure that the exercised contracts is
      // replicated while the exercise is inflight.
      val numContractsBeforeExercisedContract = numContractsInCreateBatch
      val canProceed =
        state.sentContractsCount.unwrap <= numContractsBeforeExercisedContract || canSourceProceedWithOnPR

      // Let the exercise finish after the exercise contract has been replicated.
      if (
        canSourceProceedWithOnPR && state.sentContractsCount.unwrap > numContractsBeforeExercisedContract && !canApproveExercise.isCompleted
      ) {
        logger.info(
          s"Unblocking exercise after sending ${state.sentContractsCount.unwrap} contracts"
        )
        canApproveExercise.trySuccess(())
      }
      canProceed
    }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.unsafeEnableOnlinePartyReplication(
          Map("participant1" -> (() => createSourceParticipantTestInterceptor()))
        )*
      )
      .withSetup { implicit env =>
        import env.*

        // More aggressive AcsCommitmentProcessor checking.
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = PositiveSeconds.tryOfSeconds(10).toConfig),
          )

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        alice = participant1.parties.enable("alice", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
      }

  private var partyOwners: Seq[LocalInstanceReference] = _
  private var decentralizedParty: PartyId = _
  private var previousSerial: PositiveInt = _
  private var dpToAlice: Seq[Iou.Contract] = _

  "Create decentralized party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    partyOwners = Seq[LocalInstanceReference](sequencer1, mediator1, participant3)
    decentralizedParty = createDecentralizedParty("decentralized-party", partyOwners)

    previousSerial = hostDecentralizedPartyWithCoins(
      decentralizedParty,
      partyOwners,
      participant1,
      numContractsInCreateBatch,
    )

    val amounts = (1 to numContractsInCreateBatch)
    dpToAlice = IouSyntax.createIous(participant1, decentralizedParty, alice, amounts)
    IouSyntax.createIous(participant1, alice, decentralizedParty, amounts)

    // Create some decentralized party stakeholder contracts shared with a party (Bob) already
    // on the target participant P2.
    IouSyntax.createIous(participant2, bob, decentralizedParty, amounts)
    IouSyntax.createIous(participant1, decentralizedParty, bob, amounts)
  }

  "Replicate a decentralized party" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    val (sourceParticipant, targetParticipant) = (participant1, participant2)

    val serial = previousSerial.increment

    clue("Decentralized party owners agree to have target participant co-host the party")(
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            party = decentralizedParty,
            adds = Seq((targetParticipant, ParticipantPermission.Submission)),
            store = daId,
            serial = Some(serial),
            requiresPartyToBeOnboarded = true,
          )
      )
    )

    eventually() {
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .list(daId, filterParty = decentralizedParty.filterString, proposals = true)
          .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      )
    }

    val addPartyRequestId = clue("Initiate add party async")(
      targetParticipant.parties.add_party_async(
        party = decentralizedParty,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = serial,
        participantPermission = ParticipantPermission.Submission,
      )
    )

    // Wait until the party is authorized for onboarding on the TP, before archiving replicated contracts.
    canSourceProceedWithOnPR = false
    eventually() {
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
      logger.info(s"Waiting until party onboarding topology has been authorized: $tpStatus")
      val hasConnected = tpStatus.status match {
        case AddPartyStatus.TopologyAuthorized(_, _) | AddPartyStatus.ConnectionEstablished(_, _) |
            AddPartyStatus.ReplicatingAcs(_, _, _) =>
          true
        case _ => false
      }
      hasConnected shouldBe true
    }
    val sequencer = getProgrammableSequencer(sequencer1.name)
    sequencer.setPolicy_("hold SP exercise confirmation until OnPR contract replicated") {
      submissionRequest =>
        if (
          submissionRequest.sender == sourceParticipant.id &&
          ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)
        ) {
          logger.info(
            s"Blocking exercise confirmation and unblocking OnPR progress on SP ${submissionRequest.messageId}"
          )
          canSourceProceedWithOnPR = true
          SendDecision.HoldBack(canApproveExercise.future)
        } else SendDecision.Process
    }

    val iouToExercise = dpToAlice.head
    clue(s"exercise-iou ${iouToExercise.id.contractId}, ${iouToExercise.data.amount.value}") {
      sourceParticipant.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          iouToExercise.id.exerciseCall().commands.asScala.toSeq,
          optTimeout = None, // cannot wait for TP because TP indexer paused, so only wait on SP
        )
        .discard
    }

    sequencer.resetPolicy()

    logger.info("Exercise completed. Waiting for OnPR to complete.")

    // Expect three batches owned by decentralizedParty:
    // 1. all coins plus the coin factory contract (hence the +1 below)
    // 2. Iou batch where decentralizedParty is an observer
    // 3. Iou batch where decentralizedParty is a signatory
    // The "Bob"-batches are not replicated since Bob is already on TP
    val expectedNumContracts = NonNegativeInt.tryCreate(numContractsInCreateBatch * 3 + 1)

    // Wait until both SP and TP report that party replication has completed.
    loggerFactory.assertLogsUnorderedOptional(
      eventuallyOnPRCompletes(
        sourceParticipant,
        targetParticipant,
        addPartyRequestId,
        expectedNumContracts,
      ),
      // Ignore UNKNOWN status if SP has not found out about the request yet.
      LogEntryOptionality.OptionalMany -> (_.errorMessage should include(
        "UNKNOWN/Add party request id"
      )),
    )

    // Expect all the coins to become indexed and visible via the ledger API.
    eventually() {
      val coinsAtTargetParticipant = CoinFactoryHelpers.getCoins(
        targetParticipant,
        decentralizedParty,
      )
      coinsAtTargetParticipant.size shouldBe numContractsInCreateBatch
    }
  }
}

class OnlinePartyReplicationDecentralizedPartyTestPostgres
    extends OnlinePartyReplicationDecentralizedPartyTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
