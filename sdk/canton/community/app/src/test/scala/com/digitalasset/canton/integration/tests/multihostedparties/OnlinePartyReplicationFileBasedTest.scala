// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*

/** Objective: Test file-based party replication of an external party with concurrent archiving of
  * replicated contracts.
  *
  * Setup:
  *   - 3 participants: participant1 hosts an external party to replicate online to participant2
  *     using a file with the ACS snapshot as in offline party replication
  *   - 1 mediator/sequencer each
  */
sealed trait OnlinePartyReplicationFileBasedTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with HasProgrammableSequencer
    with HasTempDirectory
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var alice: PartyId = _
  private var bob: PartyId = _

  private lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  private lazy val acsSnapshotFilename =
    tempDirectory.toTempFile(s"canton-acs-export-${this.getClass.getSimpleName}.gz").toString

  private val numContractsInCreateBatch = 100
  // false means to block OnPR (temporarily) once the TP imports a certain number of contracts
  private var canTargetProceedWithOnPR: Boolean = false
  private val canApproveExercise = Promise[Unit]()

  // Use the test interceptor to block OnPR until a concurrent exercise is processed by the SP
  // in such a way that the exercised contract is imported between exercise phases 3 and 7
  // to produce a race in the TP ConflictDetector in-memory state.
  private def createTargetParticipantTestInterceptor(): PartyReplicationTestInterceptor =
    PartyReplicationTestInterceptorImpl.targetParticipantProceedsIf { progress =>
      // Allow importing the first batch that contains the contract that this test exercises,
      // but stop before the second batch, so that we can ensure that the exercised contracts is
      // imported while the exercise is inflight.
      val numContractsBeforeExercisedContract = numContractsInCreateBatch
      val canProceed =
        progress.processedContractCount.unwrap <= numContractsBeforeExercisedContract || canTargetProceedWithOnPR

      // Let the exercise finish after the exercise contract has been imported.
      if (
        canTargetProceedWithOnPR && progress.processedContractCount.unwrap > numContractsBeforeExercisedContract && !canApproveExercise.isCompleted
      ) {
        logger.info(
          s"Unblocking exercise after importing ${progress.processedContractCount.unwrap} contracts"
        )
        canApproveExercise.trySuccess(())
      }
      canProceed
    }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.unsafeEnableOnlinePartyReplication(
          Map("participant2" -> (() => createTargetParticipantTestInterceptor()))
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

  private var externalParty: ExternalParty = _
  private var aliceToEp: Seq[Iou.Contract] = _
  private var sourceParticipant: ParticipantReference = _
  private var targetParticipant: LocalParticipantReference = _

  "Create external party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    externalParty = participant1.parties.testing.external.enable("external-party")

    val amounts = (1 to numContractsInCreateBatch)
    IouSyntax.createIous(participant1, alice, externalParty, amounts)
    val higherAmounts = amounts.map(_ + numContractsInCreateBatch)
    aliceToEp = IouSyntax.createIous(participant1, alice, externalParty, higherAmounts)

    // Create some external party stakeholder contracts shared with a party (Bob) already
    // on the target participant P2.
    IouSyntax.createIous(participant2, bob, externalParty, amounts)
  }

  // TODO(#26775): Change from Protocol.dev to Protocol.v3x
  "Replicate a party in a file-based manner" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    sourceParticipant = participant1
    targetParticipant = participant2

    // 1. In file-based OnPR, onboarding the party is authorized via topology as the first step
    // before the SP or TP begin ACS export and import.
    val authorizedOnPRSetup =
      authorizeOnboardingTopologyForFileBasedOnPR(
        sourceParticipant,
        targetParticipant,
        externalParty,
      )

    // 2. Export the ACS snapshot from SP
    sourceParticipant.parties.export_party_acs(
      externalParty,
      daId,
      targetParticipant,
      authorizedOnPRSetup.spOffsetBeforePartyOnboardingToTargetParticipant,
      acsSnapshotFilename,
    )

    // Now that the party is authorized for onboarding on the TP, archive a replicated contract.
    canTargetProceedWithOnPR = false
    val sequencer = getProgrammableSequencer(sequencer1.name)
    val firstExerciseEncounteredAndProcessed = new AtomicBoolean(false)
    sequencer.setPolicy_("hold SP second exercise confirmation until OnPR contract replicated") {
      submissionRequest =>
        if (
          submissionRequest.sender == sourceParticipant.id &&
          ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest) &&
          firstExerciseEncounteredAndProcessed.getAndSet(true)
        ) {
          logger.info(
            s"Blocking exercise confirmation and unblocking OnPR progress on TP ${submissionRequest.messageId}"
          )
          canTargetProceedWithOnPR = true
          SendDecision.HoldBack(canApproveExercise.future)
        } else SendDecision.Process
    }

    // Exercise two contracts during OnPR import to trigger the following two cases:
    // 1. The first exercise happens before the contract has been imported to ensure conflict
    //    detection can lock an unknown contract.
    // 2. The second exercise is made to coincide with contract import between phases 3 and 7
    //    to ensure that conflict detection has updated the in-memory state upon ACS import,
    //    and that in phase 7 the exercise does not encounter an inconsistency between
    //    in-memory and the DbActiveContractStore (details in #25744).
    val iousToExercise = aliceToEp.take(2)
    Future {
      iousToExercise.foreach { iouToExercise =>
        clue(s"exercise-iou ${iouToExercise.id.contractId}, ${iouToExercise.data.amount.value}") {
          sourceParticipant.ledger_api.javaapi.commands
            .submit(
              Seq(externalParty),
              iouToExercise.id.exerciseCall().commands.asScala.toSeq,
              optTimeout = None, // cannot wait for TP because TP indexer paused, so only wait on SP
            )
            .discard
        }
      }

      sequencer.resetPolicy()

      logger.info("Exercises completed. Waiting for OnPR to complete.")
    }.discard

    // 3. Have OnPR import the ACS on the TP
    val addPartyRequestId = clue("Initiate add party with acs snapshot file")(
      targetParticipant.parties.add_party_with_acs_async(
        importFilePath = acsSnapshotFilename,
        party = externalParty,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = authorizedOnPRSetup.topologySerial,
        participantPermission = ParticipantPermission.Confirmation,
      )
    )

    // Expect two IOU batches owned by externalParty
    // 1. Iou batch where externalParty is an observer
    // 2. Iou batch where externalParty is a signatory
    // The "Bob"-batches are not replicated since Bob is already on TP
    val expectedNumContracts = NonNegativeInt.tryCreate(numContractsInCreateBatch) * 2

    // Wait until TP reports that party replication has completed.
    eventuallyOnPRCompletesOnTP(targetParticipant, addPartyRequestId, Some(expectedNumContracts))
  }

  "Ensure source and target participant Ledger Api ACS match" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      eventuallyLedgerApiAcsInSyncBetweenSPAndTP(
        sourceParticipant,
        targetParticipant,
        replicatedParty = externalParty,
        limit = numContractsInCreateBatch * 10,
      )
  }

  "Ensure target participant Ledger Api ACS and internal ACS match" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      ensureActiveContractsInSyncBetweenLedgerApiAndSyncService(
        targetParticipant,
        limit = numContractsInCreateBatch * 10,
      )
  }
}

class OnlinePartyReplicationFileBasedTestH2 extends OnlinePartyReplicationFileBasedTest {
  registerPlugin(new UseH2(loggerFactory))
}

class OnlinePartyReplicationFileBasedTestPostgres extends OnlinePartyReplicationFileBasedTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
