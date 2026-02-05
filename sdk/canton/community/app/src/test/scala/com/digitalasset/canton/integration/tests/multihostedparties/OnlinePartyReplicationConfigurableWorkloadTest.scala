// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
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
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, Party, PartyId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{HasTempDirectory, config}
import monocle.macros.syntax.lens.*
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.control.NonFatal

/** Objective: Test party replication under configurable workload wrt differently hosted parties and
  * type of commands (creates, archives, exercises) to aid in customized testing during the
  * milestones of online indexing (removal of indexer pausing during OnPR).
  *
  * Setup:
  *   - 2 participants: participant1 serves as the source participant (SP), while participant2 is
  *     the target participant (TP)
  *   - SP hosts Alice (replicated to TP) and Carol, TP hosts Alice (onboarding) and Bob.
  *   - 1 mediator/sequencer each
  */
sealed trait OnlinePartyReplicationConfigurableWorkloadTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with HasTempDirectory
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))

  private var aliceE: ExternalParty = _
  private var bob: PartyId = _
  private var carol: PartyId = _
  private var sourceParticipant: ParticipantReference = _
  private var targetParticipant: LocalParticipantReference = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  private lazy val acsSnapshotFilename =
    tempDirectory.toTempFile(s"canton-acs-export-${this.getClass.getSimpleName}.gz").toString

  // Configurable workload individual switches.
  private val createContractsAlreadyOnTP = true
  private val exerciseContractsAlreadyOnTP = true
  private val createContractsOnboardingOnTP = true
  private val exerciseContractsOnboardingOnTP = true

  private val numContractsInCreateBatch = 100
  // Approximate target duration of OnPR used if necessary to slow down OnPR so that
  // at least a certain minimum of Daml workload happens concurrently to OnPR.
  private val partyReplicationApproximateMillis =
    PositiveFiniteDuration.ofSeconds(120L).duration.toMillis
  private val maxExpectedTestDuration = 4.minutes
  private val targetMillisPerContract =
    partyReplicationApproximateMillis / (numContractsInCreateBatch * 2) // * two batches (Alice+Bob, Alice+Carol)
  private val maxSizeACS = PositiveInt.tryCreate(10000)
  private val minIouAmountOfDynamicallyCreatedIOUs = 10000.0
  private val partyReplicationStartedAt = new AtomicReference[Option[Long]](None)

  // Use the test interceptor to slow down OnPR if OnPR appears to proceed too quickly for
  // enough of the workload to occur concurrently to OnPR. This deviates from the approach
  // in other tests that rely on low-volume handcrafted transactions.
  private def createTargetParticipantTestInterceptor(): PartyReplicationTestInterceptor =
    PartyReplicationTestInterceptorImpl.targetParticipantProceedsIf { state =>
      val millisNow = System.currentTimeMillis()
      partyReplicationStartedAt.get match {
        case None =>
          partyReplicationStartedAt.set(Some(millisNow))
          true
        case Some(_)
            // Don't block at the beginning or at the end
            if state.processedContractCount.unwrap == 0 || state.fullyProcessedAcs =>
          true
        case Some(millisStart) =>
          val currentMillisPerContract =
            (millisNow - millisStart) / state.processedContractCount.unwrap
          val isPartyReplicationBehindTargetDuration =
            currentMillisPerContract >= targetMillisPerContract
          logger.debug(
            s"TP processed ${state.processedContractCount} contracts, current millis per contract $currentMillisPerContract compared to target $targetMillisPerContract. Is TP behind? $isPartyReplicationBehindTargetDuration"
          )
          isPartyReplicationBehindTargetDuration
      }
    }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.unsafeEnableOnlinePartyReplication(
          Map("participant2" -> (() => createTargetParticipantTestInterceptor()))
        )*
      )
      .addConfigTransform(
        _.focus(_.parameters.timeouts.console.bounded)
          .replace(config.NonNegativeDuration.tryFromDuration(maxExpectedTestDuration))
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

        aliceE = participant1.parties.testing.external
          .enable("aliceE", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
        carol = participant1.parties.enable("carol", synchronizeParticipants = Seq(participant2))
      }

  // TODO(#26775): Change from Protocol.dev to Protocol.v3x
  "Replicate an external party via file export/import during concurrent workload" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val amounts = (1 to numContractsInCreateBatch)
      val aliceCarol = ListBuffer.from(IouSyntax.createIous(participant1, carol, aliceE, amounts))
      // Create some contracts shared with a party (Bob) already on the target participant P2.
      val aliceBob = ListBuffer.from(IouSyntax.createIous(participant2, bob, aliceE, amounts))

      sourceParticipant = participant1
      targetParticipant = participant2

      var testDone = false
      var amount: Double = minIouAmountOfDynamicallyCreatedIOUs
      @SuppressWarnings(Array("org.wartremover.warts.While"))
      val workloadF = Future {
        // Ignore racily expected NoViewWithValidRecipients submit errors due to added party
        // during transactions. As explained by the NoViewWithValidRecipients error description:
        // "It is possible that a concurrent change in the relationships between parties and
        // participants occurred during request submission. Resubmit the request."
        def ignoreSubmitError(op: => String)(code: => Unit): Unit =
          try { code }
          catch { case NonFatal(_) => logger.info(s"Unable to $op") }

        def createIouWith(owner: Party): Unit =
          ignoreSubmitError(s"create iou with owner $owner") {
            IouSyntax
              .createIou(sourceParticipant, Some(daId))(
                aliceE,
                owner,
                amount,
                optTimeout = None,
              )
              .discard
            amount = amount + 1.0
          }

        def exerciseIou(acs: ListBuffer[Iou.Contract]): Unit =
          Option
            .when(acs.nonEmpty)(acs.remove(0))
            .foreach(iou =>
              ignoreSubmitError(s"archive iou $iou") {
                IouSyntax.call(sourceParticipant, Some(daId))(iou, aliceE, optTimeout = None)
              }
            )

        while (!testDone) {
          if (createContractsAlreadyOnTP) createIouWith(bob)
          if (exerciseContractsAlreadyOnTP) exerciseIou(aliceBob)
          if (createContractsOnboardingOnTP) createIouWith(carol)
          if (exerciseContractsOnboardingOnTP) exerciseIou(aliceCarol)
        }
      }

      val authorizedOnPRSetup =
        authorizeOnboardingTopologyForFileBasedOnPR(sourceParticipant, targetParticipant, aliceE)

      sourceParticipant.parties.export_party_acs(
        aliceE,
        daId,
        targetParticipant,
        authorizedOnPRSetup.spOffsetBeforePartyOnboardingToTargetParticipant,
        acsSnapshotFilename,
      )

      try {
        loggerFactory.assertLogsUnorderedOptional(
          {
            val addPartyRequestId = clue("Initiate add party with acs snapshot file")(
              targetParticipant.parties.add_party_with_acs_async(
                importFilePath = acsSnapshotFilename,
                party = aliceE,
                synchronizerId = daId,
                sourceParticipant = sourceParticipant,
                serial = authorizedOnPRSetup.topologySerial,
                participantPermission = ParticipantPermission.Confirmation,
              )
            )

            logger.info("Waiting for OnPR to complete.")

            // Wait until TP reports that party replication has completed.
            eventuallyOnPRCompletesOnTP(
              targetParticipant,
              addPartyRequestId,
              None,
              waitAtMost = maxExpectedTestDuration,
            )
          },
          // Ignore possible error due to "concurrent change in the relationships between parties and
          // participants"
          LogEntryOptionality.OptionalMany -> (_.shouldBeCantonErrorCode(
            TransactionProcessor.SubmissionErrors.NoViewWithValidRecipients
          )),
        )
      } finally {
        testDone = true
        workloadF.futureValue
      }
  }

  "Ensure source and target participant Ledger Api ACS match" onlyRunWith ProtocolVersion.dev in {
    _ =>
      eventuallyLedgerApiAcsInSyncBetweenSPAndTP(
        sourceParticipant,
        targetParticipant,
        aliceE,
        limit = maxSizeACS,
      )
  }

  "Ensure target participant Ledger Api ACS and internal ACS match" onlyRunWith ProtocolVersion.dev in {
    _ => ensureActiveContractsInSyncBetweenLedgerApiAndSyncService(targetParticipant, maxSizeACS)
  }
}

// class OnlinePartyReplicationConfigurableWorkloadTestH2
//     extends OnlinePartyReplicationConfigurableWorkloadTest {
//   registerPlugin(new UseH2(loggerFactory))
// }

class OnlinePartyReplicationConfigurableWorkloadTestPostgres
    extends OnlinePartyReplicationConfigurableWorkloadTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
