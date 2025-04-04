// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Pruning.{
  NoWaitCommitments,
  WaitCommitments,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.IllegalArgumentError
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.{
  CommitmentsMismatch,
  NoSharedContracts,
}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.time.Duration as JDuration

trait AcsCommitmentNoWaitCounterParticipantIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers {

  val isInMemory = true

  private val interval = JDuration.ofSeconds(5)
  private val acsPruningInterval = JDuration.ofSeconds(60)
  private val minObservationDuration = NonNegativeFiniteDuration.tryOfHours(1)
  private lazy val maxDedupDuration = java.time.Duration.ofSeconds(1)

  private def checkNoWaitConfigurationsAreApplied(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    val (_, _, _, waitConfiguration) = getParticipant2WaitAndNoWaitConfiguration
    ValidateWait(waitConfiguration)
  }

  // this build the grpc answers for a given participant (both no wait and wait for easy comparison)
  private def getParticipant2WaitAndNoWaitConfiguration(implicit
      env: TestConsoleEnvironment
  ): (ParticipantId, SynchronizerId, Seq[NoWaitCommitments], Seq[WaitCommitments]) = {
    import env.*

    (
      participant2.id,
      daId,
      Seq(new NoWaitCommitments(participant2.id, Seq(daId))),
      Seq(new WaitCommitments(participant2.id, Seq(daId))),
    )
  }
  private def nowaitParticipantSynchronizerLazyDummy
      : (ParticipantId, SynchronizerId, Seq[NoWaitCommitments]) = {

    val testId = ParticipantId.apply("participant3")
    val testSynchronizer = SynchronizerId.tryFromString("da::synchronizer2")
    val noWaitList = Seq(new NoWaitCommitments(testId, Seq(testSynchronizer)))
    (testId, testSynchronizer, noWaitList)
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        def connect(
            participant: ParticipantReference,
            minObservationDuration: NonNegativeFiniteDuration,
        ): Unit = {
          // Connect and disconnect so that we can modify the synchronizer connection config afterwards
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.synchronizers.disconnect_local(daName)
          val daConfig = participant.synchronizers.config(daName).value
          participant.synchronizers.connect_by_config(
            daConfig
              .focus(_.timeTracker.minObservationDuration)
              .replace(minObservationDuration.toConfig)
          )
        }

        connect(participant1, minObservationDuration)
        connect(participant2, minObservationDuration)
        participants.all.foreach(_.dars.upload(CantonExamplesPath))
      }

  private def checkContractOnParticipants(
      iou: Iou.Contract,
      deployParticipants: Seq[LocalParticipantReference],
  ): Iou.Contract = {

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      deployParticipants.foreach(p =>
        p.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      )
    }

    iou
  }

  private def ValidateWait(
      waitList: Seq[WaitCommitments] = Seq.empty,
      noWaitList: Seq[NoWaitCommitments] = Seq.empty,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    logger.info(s"validating that noWaitList is $noWaitList and waitList is $waitList")
    val (fetchedNoWaitList, fetchedWaitList) =
      participant1.commitments.get_wait_commitments_config_from(Seq.empty, Seq.empty)
    fetchedNoWaitList should contain theSameElementsAs noWaitList
    fetchedWaitList should contain theSameElementsAs waitList
  }

  "Can get, set and reset no wait configuration" in { implicit env =>
    import env.*
    checkNoWaitConfigurationsAreApplied

    val (participant2, daId, participant2NoWaitConfig, _) =
      getParticipant2WaitAndNoWaitConfiguration

    checkNoWaitConfigurationsAreApplied

    logger.info(s"Setting no wait for participant2 on participant1")
    participant1.commitments.set_no_wait_commitments_from(Seq(participant2), Seq(daId))
    ValidateWait(noWaitList = participant2NoWaitConfig)

    logger.info(s"resetting wait for participant2 on participant1")
    participant1.commitments.set_wait_commitments_from(Seq(participant2), Seq(daId))
    checkNoWaitConfigurationsAreApplied
  }

  "Can get, set and reset no wait configuration without affecting an existing config" in {
    implicit env =>
      import env.*
      checkNoWaitConfigurationsAreApplied

      val (participant2Id, daId, participant2NoWaitConfig, participant2WaitConfig) =
        getParticipant2WaitAndNoWaitConfiguration
      val (testId, testSynchronizer, noWaitList) = nowaitParticipantSynchronizerLazyDummy

      logger.info(s"creating a fake existing no wait configuration")
      participant1.commitments.set_no_wait_commitments_from(Seq(testId), Seq(testSynchronizer))

      ValidateWait(participant2WaitConfig, noWaitList)
      logger.info(s"Setting no wait for participant2 on participant1")
      participant1.commitments.set_no_wait_commitments_from(Seq(participant2Id), Seq(daId))
      ValidateWait(noWaitList = noWaitList ++ participant2NoWaitConfig)

      logger.info(s"resetting wait for participant2 on participant1")
      participant1.commitments.set_wait_commitments_from(Seq(participant2Id), Seq(daId))
      ValidateWait(participant2WaitConfig, noWaitList)

      // clear before next test
      participant1.commitments.set_wait_commitments_from(Seq(testId), Seq(testSynchronizer))
  }

  "filtering working on get and reset" in { implicit env =>
    import env.*
    checkNoWaitConfigurationsAreApplied
    val (participant2Id, daId, _, participant2WaitConfig) =
      getParticipant2WaitAndNoWaitConfiguration
    val (testId, testSynchronizer, noWaitList) = nowaitParticipantSynchronizerLazyDummy

    logger.info(s"setting no wait for participant2 on participant1")
    participant1.commitments.set_no_wait_commitments_from(Seq(participant2Id), Seq(daId))

    logger.info(s"setting no wait for non-existing participant on participant1")
    participant1.commitments.set_no_wait_commitments_from(Seq(testId), Seq(testSynchronizer))

    logger.info(s"fetching no wait only for non-existing synchronizer")
    val (synchronizerFilterNoWaitList, synchronizerFilterWaitList) =
      participant1.commitments.get_wait_commitments_config_from(Seq(testSynchronizer), Seq.empty)
    synchronizerFilterWaitList shouldBe Seq.empty
    synchronizerFilterNoWaitList shouldBe noWaitList

    logger.info(s"fetching no wait only for non-existing participant")
    val (participantFilterNoWaitList, participantFilterWaitList) =
      participant1.commitments.get_wait_commitments_config_from(Seq.empty, Seq(testId))
    participantFilterWaitList shouldBe Seq.empty
    participantFilterNoWaitList shouldBe noWaitList

    logger.info(s"resetting wait for participant2 on participant1")
    participant1.commitments.set_wait_commitments_from(Seq(participant2Id), Seq(daId))
    ValidateWait(participant2WaitConfig, noWaitList)

    logger.info(s"resetting wait for non-existing participant on non-existing synchronizer")
    participant1.commitments.set_wait_commitments_from(Seq(testId), Seq(testSynchronizer))

    checkNoWaitConfigurationsAreApplied
  }

  "setting set wait without with missing parameters should not succeed" in { implicit env =>
    import env.*
    checkNoWaitConfigurationsAreApplied

    val (participant2Id, daId, participant2NoWaitConfig, participant2WaitConfig) =
      getParticipant2WaitAndNoWaitConfiguration
    val (testId, testSynchronizer, noWaitList) = nowaitParticipantSynchronizerLazyDummy

    logger.info(s"setting no wait for non-existing participant on participant1")
    participant1.commitments.set_no_wait_commitments_from(Seq(testId), Seq(testSynchronizer))

    logger.info(
      s"attempting to set wait on participant without a synchronizer (will fail and result should be same as previous)"
    )
    participant1.commitments.set_wait_commitments_from(Seq(participant2Id), Seq.empty)
    ValidateWait(participant2WaitConfig, noWaitList = noWaitList)

    logger.info(
      s"attempting to set wait on synchronizer without a participant (will fail and result should be same as previous)"
    )
    participant1.commitments.set_wait_commitments_from(Seq.empty, Seq(daId))
    ValidateWait(participant2WaitConfig, noWaitList = noWaitList)

    // clear before next test
    participant1.commitments.set_wait_commitments_from(Seq(testId), Seq(testSynchronizer))

  }

  "return errors on invalid input" in { implicit env =>
    import env.*
    val (participant, synchronizer, _, _) = getParticipant2WaitAndNoWaitConfiguration
    checkNoWaitConfigurationsAreApplied

    logger.info(
      s"asserting that duplicate participant and synchronizers result in an illegal argument error"
    )
    assertThrowsAndLogsCommandFailures(
      participant1.commitments
        .set_no_wait_commitments_from(
          Seq(participant, participant),
          Seq(synchronizer, synchronizer),
        ),
      _.commandFailureMessage should include(IllegalArgumentError.id),
    )

    // we check if it works when only synchronizers are duplicate
    assertThrowsAndLogsCommandFailures(
      participant1.commitments
        .set_no_wait_commitments_from(Seq(participant), Seq(synchronizer, synchronizer)),
      _.commandFailureMessage should include(IllegalArgumentError.id),
    )

    // we check if it works when only participants are duplicate
    assertThrowsAndLogsCommandFailures(
      participant1.commitments
        .set_no_wait_commitments_from(Seq(participant, participant), Seq(synchronizer)),
      _.commandFailureMessage should include(IllegalArgumentError.id),
    )

    checkNoWaitConfigurationsAreApplied
  }

  "allows idempotent addition of non-existing synchronizers and participants" in { implicit env =>
    import env.*
    checkNoWaitConfigurationsAreApplied

    val (_, _, _, participant2WaitConfig) = getParticipant2WaitAndNoWaitConfiguration
    val (testId, testSynchronizer, noWaitList) = nowaitParticipantSynchronizerLazyDummy

    logger.info(s"setting no wait for non-existing participant on non-existing synchronizer")
    participant1.commitments.set_no_wait_commitments_from(Seq(testId), Seq(testSynchronizer))
    ValidateWait(participant2WaitConfig, noWaitList)

    logger.info(s"Idempotent check")
    participant1.commitments.set_no_wait_commitments_from(Seq(testId), Seq(testSynchronizer))
    ValidateWait(participant2WaitConfig, noWaitList)

    // clear before next test
    participant1.commitments.set_wait_commitments_from(Seq(testId), Seq(testSynchronizer))
  }

  "No wait configuration allows pruning in case of ACS mismatch" onlyRunWhen (!isInMemory) in {
    implicit env =>
      import env.*
      checkNoWaitConfigurationsAreApplied

      val iou = IouSyntax
        .createIou(participant1, Some(daId))(participant1.adminParty, participant2.adminParty)

      val contractCreationTime = environment.simClock.value.now

      logger.info(s"deploying an IOU contract on participant1 and participant2")
      checkContractOnParticipants(iou, Seq(participant1, participant2))
      val contractCreationOffset =
        participant1.pruning.get_offset_by_time(contractCreationTime.toInstant)
      AdvanceTimeAndValidate(expectWarnings = false, acsPruningInterval)
      val firstPruningOffset = participant1.pruning.find_safe_offset()

      logger.info("removing the IOU from participant2")
      // this will cause acs mismatch and prevent pruning until resolved
      participant2.synchronizers.disconnect_all()
      participant2.repair.purge(daName, Seq(iou.id.toLf))
      participant2.synchronizers.reconnect_local(daName)
      // ensure participant2 is reconnected before we advance time
      eventually() {
        participant2.synchronizers.is_connected(daId) shouldBe true
        participant2.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) shouldBe empty
      }
      AdvanceTimeAndValidate(
        expectWarnings = true,
        acsPruningInterval,
        ensureParticipant1HasNoOutstanding = false,
      )

      val secondPruningOffset = participant1.pruning.find_safe_offset()

      // firstPruningOffset and secondPruningOffset should be the same since the time between first and second we have acs mismatched
      firstPruningOffset shouldBe secondPruningOffset

      logger.info("setting no wait for participant2 on participant1")
      // this will cause us to be able to prune again
      participant1.commitments.set_no_wait_commitments_from(Seq(participant2.id), Seq(daId))
      val lastPruningOffset = participant1.pruning.find_safe_offset()
      // only difference between lastPruningOffset and secondPruningOffset is the inclusion of the no-wait for participant2
      lastPruningOffset should be > secondPruningOffset
      lastPruningOffset should be > contractCreationOffset

      // clean up before next test
      participant1.synchronizers.disconnect_all()
      participant1.repair.purge(daName, Seq(iou.id.toLf))
      participant1.synchronizers.reconnect_local(daName)
      participant1.commitments.set_wait_commitments_from(Seq(participant2.id), Seq(daId))
  }

  "No wait configuration allows pruning in case of participant falling behind" onlyRunWhen (!isInMemory) in {
    implicit env =>
      import env.*
      checkNoWaitConfigurationsAreApplied
      // we need to store it, since once we stop participant2 we can not fetch it anymore
      val participant2Id = participant2.id
      val iou = IouSyntax
        .createIou(participant1, Some(daId))(participant1.adminParty, participant2.adminParty)

      val contractCreationTime = environment.simClock.value.now

      logger.info(s"deploying an IOU contract on participant1 and participant2")
      checkContractOnParticipants(iou, Seq(participant1, participant2))
      val contractCreationOffset =
        participant1.pruning.get_offset_by_time(contractCreationTime.toInstant)
      AdvanceTimeAndValidate(expectWarnings = false, acsPruningInterval)
      val firstPruningOffset = participant1.pruning.find_safe_offset()

      logger.info("stopping participant2")
      // this will prevent pruning since participant2 is falling behind
      participant2.stop()
      eventually() {
        participant2.health.is_running() shouldBe false
      }
      AdvanceTimeAndValidate(
        expectWarnings = false,
        acsPruningInterval,
        ensureParticipant1HasNoOutstanding = false,
      )
      val secondPruningOffset = participant1.pruning.find_safe_offset()

      logger.info("setting no wait for participant2 on participant1")
      // this will cause us to be able to prune again
      participant1.commitments.set_no_wait_commitments_from(Seq(participant2Id), Seq(daId))
      val lastPruningOffset = participant1.pruning.find_safe_offset()
      // firstPruningOffset and secondPruningOffset should be the same since the time between first and second we have acs mismatched
      firstPruningOffset shouldBe secondPruningOffset
      // only difference between lastPruningOffset and secondPruningOffset is the inclusion of the no-wait for participant2
      lastPruningOffset should be > secondPruningOffset
      lastPruningOffset should be > contractCreationOffset
  }

  private def AdvanceTimeAndValidate(
      expectWarnings: Boolean,
      timeAdvance: JDuration,
      ensureParticipant1HasNoOutstanding: Boolean = true,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    lazy val method: Unit = {
      val simClock = environment.simClock.value
      val startTime = simClock.now
      simClock.advance(timeAdvance)
      val endTime = simClock.now
      logger.info(s"advancing simClock from $startTime to $endTime")
      logger.info("performing ping and updating time on active synchronizers")
      participant1.health.ping(participant1)
      participants.all.filter(_.health.is_running()).foreach(_.testing.fetch_synchronizer_times())

      if (ensureParticipant1HasNoOutstanding) {
        eventually() {
          // we add 1 micro since start is inclusive
          participant1.commitments.outstanding(
            daName,
            startTime.addMicros(1).toInstant,
            endTime.toInstant,
          ) shouldBe empty
          participant1.commitments.received(
            daName,
            startTime.addMicros(1).toInstant,
            endTime.toInstant,
          ) should not be empty
        }
      }
    }

    // since we break the ACS with the repair we expect mismatch and no share after the repair command
    if (expectWarnings)
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        method,
        logs => {
          forAtLeast(1, logs)(m => m.message should include(CommitmentsMismatch.id))
          forAtLeast(1, logs)(m => m.message should include(NoSharedContracts.id))
        },
      )
    else
      method

  }

}

class AcsCommitmentNoWaitCounterParticipantIntegrationTestPostgres
    extends AcsCommitmentNoWaitCounterParticipantIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  override val isInMemory = false
}
class AcsCommitmentNoWaitCounterParticipantIntegrationTestDefault
    extends AcsCommitmentNoWaitCounterParticipantIntegrationTest {}
