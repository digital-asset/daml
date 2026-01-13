// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

/** The goal of this test is to ensure that requests that are in-flight at the time of the logical
  * synchronizer upgrade time out.
  *
  * We consider two requests:
  *   - One that does not reach phase 3 (confirmation request dropped by the sequencer), submitted
  *     by participant1. The time out of such a request is done when the ledger end/clean sequencer
  *     index reaches the max sequencing time. Hence, this is triggered by time progressing on the
  *     successor (after the upgrade is done).
  *
  *   - One that reaches phase 3 (confirmation response dropped), submitted by participant2. Because
  *     of the offset applied to sequencing times after the upgrade time, the witnessed time after
  *     the upgrade time on the old synchronizer triggers such a time out (before the upgrade is
  *     done).
  */
final class LSUTimeoutInFlightIntegrationTest extends LSUBase with HasProgrammableSequencer {

  override protected def testName: String = "lsu-timeout-in-flight"

  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private var decisionTimeout: config.NonNegativeFiniteDuration = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }
      .withSetup { implicit env =>
        import env.*
        decisionTimeout = sequencer1.topology.synchronizer_parameters.latest(daId).decisionTimeout

        // Ensures the decision time is sufficiently big that there is something meaningful to be measured
        decisionTimeout should be > config.NonNegativeFiniteDuration.ofSeconds(10)
      }

  private def fetchTime(s: LocalSequencerReference): CantonTimestamp =
    s.underlying.value.sequencer.timeTracker.fetchTime().futureValueUS

  "In-flight requests" should {
    "be timed out around LSU" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      val alice = participant1.parties.enable("alice")
      val bob = participant2.parties.enable("bob")

      val iou1 = IouSyntax.createIou(participant1)(alice, bob)
      val iou2 = IouSyntax.createIou(participant2)(bob, alice)

      getProgrammableSequencer(sequencer1.name).setPolicy_("drop some messages") {
        submissionRequest =>
          val isP1ConfirmationRequest =
            submissionRequest.isConfirmationRequest && submissionRequest.sender == participant1.id
          val isP2ConfirmationResponse = ProgrammableSequencerPolicies.isConfirmationResponse(
            submissionRequest
          ) && submissionRequest.sender == participant2.id

          if (isP1ConfirmationRequest || isP2ConfirmationResponse)
            SendDecision.Drop
          else SendDecision.Process
      }

      val ledgerEndP1 = participant1.ledger_api.state.end()
      val ledgerEndP2 = participant2.ledger_api.state.end()

      val archive1F = Future {
        participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), iou1.id.exerciseArchive().commands().asScala.toSeq)
      }

      val archive2F = Future {
        participant2.ledger_api.javaapi.commands
          .submit(Seq(bob), iou2.id.exerciseArchive().commands().asScala.toSeq)
      }

      val checkedLogEntries = LogEntry.assertLogSeq(
        Seq(
          // p1 confirmation request dropped
          (
            _.warningMessage should include("Submission timed out at"),
            "participant confirmation request timed out",
          ),
          // rejection of iou1 archival
          (
            _.errorMessage should include(
              "Transaction was not sequenced within the pre-defined max sequencing time and has therefore timed out"
            ),
            "command 1 submission fails",
          ),
          // p2 confirmation response dropped
          (
            _.warningMessage should (include("Response message for request") and include(
              "timed out at"
            )),
            "participant confirmation response timed out",
          ),
          // rejection of iou2 archival
          (
            _.errorMessage should include(
              "Rejected transaction due to a participant determined timeout"
            ),
            "command 2 submission fails",
          ),
        )
      )(_)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          eventually() {
            // Request 1 is considered as unsequenced
            participant1.underlying.value.sync.participantNodePersistentState.value.inFlightSubmissionStore
              .lookupUnsequencedUptoUnordered(daId, CantonTimestamp.MaxValue)
              .futureValueUS
              .loneElement

            // Request 2 is in-flight from p1 and p2 point of view
            participant1.underlying.value.sync
              .connectedSynchronizerForAlias(daName)
              .value
              .numberOfDirtyRequests() shouldBe 1
            participant2.underlying.value.sync
              .connectedSynchronizerForAlias(daName)
              .value
              .numberOfDirtyRequests() shouldBe 1
          }

          performSynchronizerNodesLSU(fixture)

          fetchTime(sequencer1) should be < upgradeTime
          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          // We don't want the old mediator to interact with the sequencer
          mediator1.stop()

          eventually() {
            participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
          }

          archive2F.failed.futureValue shouldBe a[Throwable]
          participant2.ledger_api.completions
            .list(bob, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEndP2)
            .loneElement
            .status
            .value shouldBe LocalRejectError.TimeRejects.LocalTimeout
            .Reject()
            .rpcStatusWithoutLoggingContext()

          eventually() {
            participant1.underlying.value.sync
              .connectedSynchronizerForAlias(daName)
              .value
              .numberOfDirtyRequests() shouldBe 0
            participant2.underlying.value.sync
              .connectedSynchronizerForAlias(daName)
              .value
              .numberOfDirtyRequests() shouldBe 0
          }

          // Check that time offsetting after upgrade works on old synchronizer
          fetchTime(sequencer1) should be > (upgradeTime + decisionTimeout.toInternal)

          // And also after a restart
          sequencer1.stop()
          sequencer1.start()
          fetchTime(sequencer1) should be > (upgradeTime + decisionTimeout.toInternal)

          // TODO(#27349) Without this ping, the fetch_time allowing to timeout request 1 is never done
          participant1.health.ping(participant1)

          environment.simClock.value.advance(
            sequencer1.topology.synchronizer_parameters
              .latest(daId)
              .ledgerTimeRecordTimeTolerance
              .asJava
          )

          participant1.ledger_api.completions
            .list(alice, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEndP1)
            .loneElement
            .status
            .value
            .message should include(SubmissionErrors.TimeoutError.Error().cause)

          archive1F.failed.futureValue

        },
        checkedLogEntries,
      )
    }
  }
}
