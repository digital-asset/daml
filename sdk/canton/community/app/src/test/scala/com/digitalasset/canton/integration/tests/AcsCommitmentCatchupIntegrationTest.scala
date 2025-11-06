// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.NonNegativeProportion
import com.digitalasset.canton.config.{CommitmentSendDelay, DbConfig}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.util.{CommitmentTestUtil, IntervalDuration}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference

sealed trait AcsCommitmentCatchupIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with CommitmentTestUtil
    with HasProgrammableSequencer {

  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)

  private val alreadyDeployedContracts12: AtomicReference[Seq[Iou.Contract]] =
    new AtomicReference[Seq[Iou.Contract]](Seq.empty)
  private val alreadyDeployedContracts23: AtomicReference[Seq[Iou.Contract]] =
    new AtomicReference[Seq[Iou.Contract]](Seq.empty)

  private lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.enableAdditionalConsistencyChecks)
            .replace(true)
        ),
      )
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              Some(NonNegativeProportion.zero),
              Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronisation.await_idle()
        sequencer2.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        initializedSynchronizers.foreach { case (alias, synchronizer) => synchronizer }

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.foreach(_.dars.upload(CantonExamplesPath, synchronizerId = daId))
        passTopologyRegistrationTimeout(env)
      }

  "Commitment catch-up" should {
    "not trigger when the participant shouldn't implicitly have sent a commitments" in {
      implicit env =>
        import env.*

        val simClock = environment.simClock.value

        logger.debug(s"P1 and P2 share a contract and exchange a commitments")
        val (cids12da, period12da, commitment12da) =
          deployOneAndCheck(daId, alreadyDeployedContracts12, participant1, participant2)

        val catchUpParam = initializedSynchronizers(daName).synchronizerOwners.headOption
          .getOrElse(fail("synchronizer owners are missing"))
          .topology
          .synchronizer_parameters
          .get_dynamic_synchronizer_parameters(daId)
          .acsCommitmentsCatchUp
          .getOrElse(
            throw new IllegalStateException("Cannot retrieve acs commitment catch-up parameters")
          )

        // We advance the clock by catchUpParam.catchUpIntervalSkip intervals
        val catchUpThreshold = interval.multipliedBy(
          catchUpParam.catchUpIntervalSkip.value.toLong * catchUpParam.nrIntervalsToTriggerCatchUp.value.toLong
        )
        simClock.advanceTo(simClock.now.add(catchUpThreshold))

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          {

            logger.debug(
              s"P2 and P3 share a contract and exchange a commitment. P2 also sends commitments to P1, because" +
                s"they still share a contract. That makes P1 appear to be behind by the catch up threshold. However, P1" +
                s"shouldn't trigger catch-up mode because it is not actually behind."
            )
            val (cids23da, period23da, commitment23da) =
              deployOneAndCheck(daId, alreadyDeployedContracts23, participant2, participant3)

            val synchronizerId = daId
            val endTimestamp = period23da.toInclusive.forgetRefinement
            participant1.commitments.lookup_sent_acs_commitments(
              synchronizerTimeRanges = Seq(
                SynchronizerTimeRange(
                  synchronizerId,
                  Some(TimeRange(endTimestamp.minusMillis(1), endTimestamp)),
                )
              ),
              counterParticipants = Seq.empty,
              commitmentState = Seq.empty,
              verboseMode = true,
            )
            // user-manual-entry-end: InspectSentCommitments
            eventually() {
              val p1Computed = participant1.commitments.lookup_sent_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    synchronizerId,
                    Some(TimeRange(endTimestamp.minusMillis(1), endTimestamp)),
                  )
                ),
                counterParticipants = Seq.empty,
                commitmentState = Seq.empty,
                verboseMode = true,
              )

              logger.debug(
                "P1 sent commitments only for synchronizer da, so the result size should be 1"
              )
              p1Computed.size shouldBe 1
              val daCmts = p1Computed.get(daId).value
              logger.debug("P1 sent two commitments for synchronizer da")
              daCmts.size shouldBe 1
              daCmts(0).destCounterParticipant shouldBe participant2.id

              p1Computed
            }
          },
          logs =>
            forAll(logs) { log =>
              log.message should not include ("Modes: in catch-up mode = true")
            },
        )
    }
  }
}

class AcsCommitmentCatchupIntegrationTestPostgres extends AcsCommitmentCatchupIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

//class AcsCommitmentCatchupIntegrationTestH2 extends AcsCommitmentCatchupIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer(
//        Seq(
//          Set(InstanceName.tryCreate("sequencer1")),
//          Set(InstanceName.tryCreate("sequencer2")),
//        )
//      ),
//    )
//  )
//}
