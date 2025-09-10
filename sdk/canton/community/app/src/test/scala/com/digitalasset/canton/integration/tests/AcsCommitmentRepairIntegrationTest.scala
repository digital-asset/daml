// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, NonNegativeDuration}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.util.{CommitmentTestUtil, IntervalDuration}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.ReceivedCmtState.Mismatch
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.topology.SynchronizerId
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

trait AcsCommitmentRepairIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with CommitmentTestUtil {

  private val iouContract = new AtomicReference[Iou.Contract]
  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)

  private var alreadyDeployedContracts: Seq[Iou.Contract] = Seq.empty

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
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
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

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.foreach(_.dars.upload(CantonExamplesPath))
        passTopologyRegistrationTimeout(env)
      }

  def createContractsAndCheck(synchronizerId: SynchronizerId)(implicit
      env: FixtureParam
  ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) = {
    import env.*
    val nContracts = PositiveInt.three
    val simClock = environment.simClock.value
    simClock.advanceTo(simClock.uniqueTime().immediateSuccessor)

    val createdCids =
      (1 to nContracts.value).map(_ => deployOnP1P2AndCheckContract(synchronizerId, iouContract))

    val tick1 = tickAfter(simClock.uniqueTime())
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)

    participant1.testing.fetch_synchronizer_times()

    val p1Computed = eventually() {
      val p1Computed = participant1.commitments
        .computed(
          daName,
          tick1.toInstant.minusMillis(1),
          tick1.toInstant,
          Some(participant2),
        )
      p1Computed.size shouldBe 1
      p1Computed
    }

    val (period, _participant, commitment) = p1Computed.loneElement
    alreadyDeployedContracts = alreadyDeployedContracts.concat(createdCids)
    (createdCids, period, commitment)
  }

  "Running commitment reinitialization" should {
    "reinitialize commitments based on ACS" in { implicit env =>
      import env.*

      val simClock = environment.simClock.value

      // Deploy three contracts. P1 and P2 exchange commitments
      createContractsAndCheck(daId)

      // P1 reinitializes commitments on da and acme. We should see the reinit in the DB, but no errors or warnings
      // in particular regarding inconsistencies or commitment mismatches.
      val ts = simClock.now
      val reinitCmtsResult =
        participant1.commitments.reinitialize_commitments(
          Seq.empty,
          Seq.empty,
          Seq.empty,
          NonNegativeDuration.ofSeconds(30),
        )
      // We should have results for both synchronizers and they should have the reinit timestamp defined and equal or greater than
      // the time just before the command
      reinitCmtsResult.map(_.synchronizerId) should contain theSameElementsAs Seq(
        daId.logical,
        acmeId.logical,
      )
      forAll(reinitCmtsResult)(_.acsTimestamp.isDefined shouldBe true)
      forAll(reinitCmtsResult)(_.acsTimestamp.value shouldBe >=(ts))
      // exchange commitments again, all should be fine
      createContractsAndCheck(daId)
      createContractsAndCheck(acmeId)

      // Corrupt P2's running commitments on da by emptying them in the DB. We do that while disconnecting P2 from da
      // so upon reconnect P2 initializes its running commitments from the DB. We expect that commitment exchange results
      // in mismatches, and we detect inconsistencies between the running commitments and the ACS
      participant2.synchronizers.disconnect_all()
      eventually() {
        participant2.synchronizers.list_connected() shouldBe empty
      }
      // eventually running commitments are changed in the DB
      val corruptRunningCommitments = participant2.underlying.value.sync.syncPersistentStateManager
        .acsCommitmentStore(daId)
        .map(_.runningCommitments)
        .value
      corruptRunningCommitments
        .get()
        .flatMap { case (recordTime, runningCmts) =>
          corruptRunningCommitments.update(
            recordTime,
            updates = Map.empty,
            deletes = runningCmts.keySet,
          )
        }
        .futureValueUS

      // catch warnings about commitment mismatch and inconsistencies between running commitments and ACS
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          participant2.synchronizers.reconnect_all()
          eventually() {
            participant2.synchronizers
              .list_connected()
              .map(_.physicalSynchronizerId) should contain(daId)
          }

          // exchange commitments
          val (_, period3da, _) = createContractsAndCheck(daId)
          eventually() {
            val p1Received = participant1.commitments.lookup_received_acs_commitments(
              synchronizerTimeRanges = Seq(
                SynchronizerTimeRange(
                  daId,
                  Some(
                    TimeRange(
                      period3da.fromExclusive.forgetRefinement,
                      period3da.toInclusive.forgetRefinement,
                    )
                  ),
                )
              ),
              counterParticipants = Seq.empty,
              commitmentState = Seq(Mismatch),
              verboseMode = false,
            )

            val daCmtsP1 = p1Received.get(daId).value
            daCmtsP1.size shouldBe (1)

            val p2Received = participant2.commitments.lookup_received_acs_commitments(
              synchronizerTimeRanges = Seq(
                SynchronizerTimeRange(
                  daId,
                  Some(
                    TimeRange(
                      period3da.fromExclusive.forgetRefinement,
                      period3da.toInclusive.forgetRefinement,
                    )
                  ),
                )
              ),
              counterParticipants = Seq.empty,
              commitmentState = Seq(Mismatch),
              verboseMode = false,
            )

            val daCmtsP2 = p2Received.get(daId).value
            daCmtsP2.size shouldBe (1)
          }

          // Repair P2's commitments on da by reinitializing them based on the ACS. We expect that the running commitments
          // are repaired, and there are no more inconsistencies or commitments mismatches
          val reinitCmtsResult2 =
            participant2.commitments.reinitialize_commitments(
              Seq(daId),
              Seq.empty,
              Seq.empty,
              NonNegativeDuration.ofSeconds(30),
            )
          // results should be just for daId, and the timestamp should be defined

          reinitCmtsResult2.map(_.synchronizerId) should contain theSameElementsAs Seq(daId.logical)
          forAll(reinitCmtsResult2)(_.acsTimestamp.isDefined shouldBe true)
        },
        logs => {
          forAtLeast(1, logs) { m =>
            m.message should include(CommitmentsMismatch.id)
          }
          forAtLeast(1, logs) { m =>
            m.message should include(
              "Detected an inconsistency between the running commitment and the ACS"
            )
          }
        },
      )

      // the commitment should match the returned one
      val (_, period4da, _) = createContractsAndCheck(daId)
      eventually() {
        val p1Received = participant1.commitments.lookup_received_acs_commitments(
          synchronizerTimeRanges = Seq(
            SynchronizerTimeRange(
              daId,
              Some(
                TimeRange(
                  period4da.fromExclusive.forgetRefinement,
                  period4da.toInclusive.forgetRefinement,
                )
              ),
            )
          ),
          counterParticipants = Seq.empty,
          commitmentState = Seq.empty,
          verboseMode = false,
        )

        val daCmts = p1Received.get(daId).value

        daCmts.size shouldBe 1
      }
    }

    def reinitCommitments(participant: LocalParticipantReference) =
      participant.commitments.reinitialize_commitments(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NonNegativeDuration.ofSeconds(30),
      )

    "not be scheduled if a reinitialization is in progress" in { implicit env =>
      import env.*

      // Launch many reinitializations in parallel. Some of these should fail because likely another reinit is in
      // progress. This is a weaker condition than "no two reinitializations are ever scheduled in parallel",
      // but it's good enough because (1) Checking the stricter condition is flaky unless we invest quite a bit of effort,
      // or we increase the CI cost, and (2) The logic we're testing isn't terribly complex.
      val manyReinits = Future.traverse(1 to 10)(_ => Future(reinitCommitments(participant1)))

      val res = for {
        resManyReinits <- manyReinits
        _ = resManyReinits.flatten.map(
          _.error
        ) should contain(
          Some(
            s"Reinitialization is already scheduled or in progress for $daId."
          )
        )
        // after all reinits return, another reinit should succeed
        successfulReinit = participant1.commitments.reinitialize_commitments(
          Seq(daId.logical),
          Seq.empty,
          Seq.empty,
          NonNegativeDuration.ofSeconds(30),
        )
        _ = forAll(successfulReinit.map(_.error shouldBe None))
      } yield ()
      res.futureValue
    }
  }
}

class AcsCommitmentRepairIntegrationTestPostgres extends AcsCommitmentRepairIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
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

class AcsCommitmentRepairIntegrationTestH2 extends AcsCommitmentRepairIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
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
