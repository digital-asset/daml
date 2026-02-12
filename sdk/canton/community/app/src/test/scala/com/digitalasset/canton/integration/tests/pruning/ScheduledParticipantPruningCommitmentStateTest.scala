// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.BackgroundWorkloadRunner
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.scheduler.IgnoresTransientSchedulerErrors
import com.digitalasset.canton.scheduler.SafeToPruneCommitmentState.{All, Match, MatchOrMismatch}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MemberRecipient,
  Recipients,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration as JDuration

class ScheduledParticipantPruningCommitmentStateTestPostgres
    extends ScheduledParticipantPruningCommitmentStateTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

abstract class ScheduledParticipantPruningCommitmentStateTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ConfiguresScheduledPruning
    with IgnoresTransientSchedulerErrors
    with BackgroundWorkloadRunner
    with HasProgrammableSequencer {

  private val reconciliationInterval = JDuration.ofSeconds(1)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  private val internalPruningBatchSize =
    PositiveInt.tryCreate(5) // small enough to exercise batching of prune requests
  private val maxDedupDuration =
    JDuration.ofSeconds(1) // Low so that we don't delay pruning unnecessarily

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.updatePruningBatchSize(internalPruningBatchSize),
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.stores.safeToPruneCommitmentState)
            .replace(Some(Match))
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.stores.safeToPruneCommitmentState)
            .replace(Some(MatchOrMismatch))
        ),
        ConfigTransforms.updateParticipantConfig("participant3")(
          _.focus(_.parameters.stores.safeToPruneCommitmentState)
            .replace(Some(All))
        ),
      )
      .withSetup { env =>
        import env.*
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
            reconciliationInterval = PositiveDurationSeconds(reconciliationInterval),
          ),
        )
      }

  "Able to run scheduled participant pruning taking into account the commitment state" in {
    implicit env =>
      verifyScheduledPruning()
  }

  def verifyScheduledPruning()(implicit env: TestConsoleEnvironment): Any = {
    import env.*
    Seq(participant1, participant2, participant3).foreach { p =>
      p.synchronizers.connect_local(sequencer1, alias = daName)
      p.dars.upload(CantonTestsPath)
    }
    eventually()(
      assert(Seq(participant1, participant2, participant3).forall(_.synchronizers.active(daName)))
    )

    def deployContract() = {
      logger.info(s"Deploying an iou contract on all three participants")
      val iou = IouSyntax
        .createIou(participant1, Some(daId))(
          participant1.adminParty,
          participant2.adminParty,
          observers = List(participant3.adminParty),
        )

      eventually() {
        Seq(participant1, participant2, participant3).foreach(p =>
          p.ledger_api.state.acs
            .of_all()
            .filter(_.contractId == iou.id.contractId) should not be empty
        )
      }
    }

    def checkSequencedMessage(
        submissionRequest: SubmissionRequest,
        expectedSender: ParticipantReference,
        receivers: ParticipantReference*
    ): Boolean = {
      val expectedReceivers = receivers.map(ref => MemberRecipient(ref.member)).toSet
      submissionRequest.sender == expectedSender.member && submissionRequest.batch.allRecipients == expectedReceivers
    }

    def tamperWithCommitmentInSubmissionRequest(
        req: SubmissionRequest,
        src: LocalParticipantReference,
        dst: Seq[LocalParticipantReference],
    ) = {

      val defaultPeriod = CommitmentPeriod
        .create(
          CantonTimestamp.Epoch,
          CantonTimestamp.Epoch.plusSeconds(reconciliationInterval.toSeconds),
          PositiveSeconds.create(reconciliationInterval).value,
        )
        .value

      val period = req.batch.envelopes.headOption
        .map { envelope =>
          val openEnvelope = envelope
            .openEnvelope(src.crypto.pureCrypto, testedProtocolVersion)
            .valueOrFail("open envelopes")
            .protocolMessage
          openEnvelope match {
            case SignedProtocolMessage(typedMessage, _signatures) =>
              typedMessage.content match {
                case AcsCommitment(_psid, _sender, _counterParticipant, period, _commitment) =>
                  period
                case _ => defaultPeriod
              }
            case _ =>
              fail(
                "tamperWithCommitment should only be called on a submission request that contains an ACS commitment"
              )
          }
        }
        .getOrElse(defaultPeriod)

      val srcTopologySnapshot = src.underlying.value.sync.syncCrypto
        .tryForSynchronizer(daId, staticSynchronizerParameters1)
        .currentSnapshotApproximation

      val signedFakeMsgs = dst.map { case recipient =>
        val commitment1 = AcsCommitment
          .create(
            daId,
            src,
            recipient,
            period,
            LtHash16().getByteString(),
            testedProtocolVersion,
          )

        val signedFakeMsg =
          SignedProtocolMessage
            .trySignAndCreate(
              commitment1,
              srcTopologySnapshot.futureValueUS,
            )
            .map(_ -> Recipients.cc(recipient))
            .futureValueUS
        signedFakeMsg
      }

      val batch = Batch.of(testedProtocolVersion, signedFakeMsgs*)

      val modifiedRequest = req
        .focus(_.batch)
        .replace(Batch.closeEnvelopes(batch))

      modifiedRequest
    }

    val sequencer = getProgrammableSequencer(sequencer1.name)
    sequencer.setPolicy_(
      "Block all commitments from p2 to p1 and p3, and alter commitments from p3 to p1 and p2"
    ) { req =>
      if (ProgrammableSequencerPolicies.isAcsCommitment(req)) {
        if (checkSequencedMessage(req, participant2, participant1, participant3)) {
          SendDecision.Drop
        } else if (checkSequencedMessage(req, participant3, participant1, participant2)) {

          val modifiedRequest =
            tamperWithCommitmentInSubmissionRequest(
              req,
              participant3,
              Seq(participant1, participant2),
            )

          val signedModifiedRequest = signModifiedSubmissionRequest(
            modifiedRequest,
            participant3.underlying.value.sync.syncCrypto
              .tryForSynchronizer(daId, staticSynchronizerParameters1),
          )
          SendDecision.Replace(signedModifiedRequest)
        } else SendDecision.Process
      } else SendDecision.Process
    }

    def ledgerBeginOffset(p: LocalParticipantReference): Long =
      p.testing.state_inspection.prunedUptoOffset
        .fold(0L)(_.unwrap)

    def ledgerEndOffset(p: LocalParticipantReference): Long =
      p.ledger_api.state.end()

    ignoreTransientSchedulerErrorsAndCommitmentMismatches(this.getClass.getSimpleName) {
      deployContract()
      val ledgerEndAfter1 = ledgerEndOffset(participant1)
      val ledgerEndAfter2 = ledgerEndOffset(participant2)
      val ledgerEndAfter3 = ledgerEndOffset(participant3)

      val pruningIntervalP1 = 2
      val pruningIntervalP2 = 3
      val retentionP1 = 2
      val retentionP2 = 3
      val maxDurationP1 = 1
      val maxDurationP2 = 2

      Seq(
        (participant1, pruningIntervalP1, maxDurationP1, retentionP1),
        (participant2, pruningIntervalP2, maxDurationP2, retentionP2),
        (participant3, pruningIntervalP2, maxDurationP2, retentionP2),
      )
        .foreach { case (p, interval, maxDuration, retention) =>
          val pruningSchedule = PruningSchedule(
            s"/$interval * * * * ? *",
            config.PositiveDurationSeconds.ofSeconds(maxDuration.toLong),
            config.PositiveDurationSeconds.ofSeconds(retention.toLong),
          )
          setAndVerifyPruningSchedule(p.pruning, pruningSchedule)
        }

      val secondsToRunTest = 20

      (1 to secondsToRunTest).foreach { _ =>
        Threading.sleep(1000)
        // advance ledger end
        deployContract()
      }

      Seq(participant1, participant2, participant3).foreach(_.pruning.clear_schedule())

      val lbo1 = ledgerBeginOffset(participant1)
      val lbo2 = ledgerBeginOffset(participant2)
      val lbo3 = ledgerBeginOffset(participant3)
      // Participant 1 receives a modified commitment and has an outstanding one, and can't prune on a Match policy,
      // thus its ledger begin offset doesn't advance
      lbo1 shouldBe <=(ledgerEndAfter1)
      // Participant 2 receives a matching commitment and a mismatching one, and can prune on a MatchOrMismatch policy
      lbo2 shouldBe >=(ledgerEndAfter2)
      // Participant 3 receives a matching commitment and has an outstanding one, and can prune on an All policy
      lbo3 shouldBe >=(ledgerEndAfter3)

      sequencer.resetPolicy()
    }
  }
}
