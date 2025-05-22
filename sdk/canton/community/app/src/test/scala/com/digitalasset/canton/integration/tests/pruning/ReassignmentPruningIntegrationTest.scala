// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds, StorageConfig}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  EntitySyntax,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, config}

import java.time.Duration as JDuration
import scala.concurrent.duration.DurationInt

sealed trait ReassignmentPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasCycleUtils
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  private var alice: PartyId = _
  private var bank: PartyId = _
  private var offsetP1AfterUnassign: Long = _
  private var offsetP2AfterUnassign: Long = _
  private var offsetP1AfterAssign: Long = _
  private var offsetP2AfterAssign: Long = _

  // These three parameters are needed to be able to wait sufficiently long to trigger a pruning timeout
  private val reconciliationInterval = JDuration.ofSeconds(10)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  // Pick a low max dedup duration so that we don't delay pruning unnecessarily
  private val maxDedupDuration = JDuration.ofSeconds(5)
  private val pruningTimeout =
    Ordering[JDuration].max(
      reconciliationInterval
        .plus(confirmationResponseTimeout.duration)
        .plus(mediatorReactionTimeout.duration),
      maxDedupDuration,
    )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .withSetup { implicit env =>
        import env.*

        Seq((sequencer1, daId), (sequencer2, acmeId)).foreach { case (sequencer, synchronizerId) =>
          // Disable automatic assignment
          sequencer.topology.synchronizer_parameters.propose_update(
            synchronizerId,
            _.update(
              assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
              reconciliationInterval = PositiveDurationSeconds(reconciliationInterval),
              confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
              mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
            ),
          )
        }

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

        participant1.health.ping(participant2.id)

        alice = participant1.parties.enable("alice", synchronizer = daName)
        participant1.parties.enable("alice", synchronizer = acmeName)
        bank = participant2.parties.enable("bank", synchronizer = daName)
        participant2.parties.enable("bank", synchronizer = acmeName)

        participants.all.dars.upload(BaseTest.CantonExamplesPath)
        participant1.health.ping(participant2.id)
      }

  private def ensureOffsetSafeToPrune(
      desiredPruningOffset: Long,
      clock: SimClock,
      participant: LocalParticipantReference,
  ): Unit = {
    eventually() {
      val safeOffset = participant.pruning
        .find_safe_offset(clock.now.toInstant)
        .value
      val safeOffset2 = desiredPruningOffset
      safeOffset should be >= safeOffset2
    }
    participant.pruning.prune(desiredPruningOffset)
  }

  private def ensureOffsetUnsafeToPrune(
      undesiredPruningOffset: Long,
      clock: SimClock,
      participant: LocalParticipantReference,
  ): Unit = {
    eventually() {
      val safeOffset = participant.pruning
        .find_safe_offset(clock.now.toInstant)
        .value
      safeOffset should be < undesiredPruningOffset
    }

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant.pruning.prune(undesiredPruningOffset),
      logEntry =>
        logEntry.errorMessage should include(
          "GrpcRequestRefusedByServer: FAILED_PRECONDITION/UNSAFE_TO_PRUNE"
        ),
    )
  }

  // Creates on synchronizer an Iou with Alice as owner and the Bank as the payer
  private def createIou(
      synchronizerId: SynchronizerId
  )(implicit env: TestConsoleEnvironment): Iou.ContractId = {
    import env.*
    clue(s"create Iou contract on $daId") {
      IouSyntax.createIou(env.participant2, Some(synchronizerId))(bank, alice).id
    }
  }

  // unassigns the given iou from origin to target
  // returns the unassignmentId, the unassignment offset on participant1 and the unassignment offset on participant2
  private def unassignIou(
      origin: SynchronizerId,
      target: SynchronizerId,
      contractId: Iou.ContractId,
  )(implicit
      env: TestConsoleEnvironment
  ): (String, Long, Long) = {
    import env.*

    val ledgerEndP1BeforeUnassign =
      participant1.ledger_api.state.end()
    val unassignment =
      participant2.ledger_api.commands.submit_unassign(
        bank,
        Seq(contractId.toLf),
        origin,
        target,
      )
    val unassignOffsetP2 = unassignment.reassignment.offset
    val unassignmentId = unassignment.unassignId

    val unassignOffsetP1 = participant1.ledger_api.updates
      .trees(Set(alice), 1, ledgerEndP1BeforeUnassign)
      .collectFirst { case wrapper: UpdateService.ReassignmentWrapper =>
        wrapper.reassignment.offset
      }
      .value
    (unassignmentId, unassignOffsetP1, unassignOffsetP2)
  }

  // assigns the given iou from origin to target
  // returns the assignment offset on participant1 and the assignment offset on participant2
  private def assignIou(origin: SynchronizerId, target: SynchronizerId, unassignmentId: String)(
      implicit env: TestConsoleEnvironment
  ): (Long, Long) = {
    import env.*

    val ledgerEndP2BeforeAssign =
      participant2.ledger_api.state.end()
    val res = participant1.ledger_api.commands.submit_assign(alice, unassignmentId, origin, target)
    val assignOffsetP1 = res.reassignment.offset

    val assignOffsetP2 = participant2.ledger_api.updates
      .trees(Set(bank), 1, ledgerEndP2BeforeAssign)
      .collectFirst { case wrapper: UpdateService.ReassignmentWrapper =>
        wrapper.reassignment.offset
      }
      .value

    (assignOffsetP1, assignOffsetP2)
  }

  "pruning on reassignments should" should {
    "prune completed reassignments, not prune incomplete ones" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      // create iou on daId with Alice and the Bank as stakeholders
      val contractId = createIou(daId)

      // Prepare reassignment of the Iou from daId to acmeId
      // First unassign the Iou from daId to acmeId
      val (unassignmentId, unassignOffsetP1, unassignOffsetP2) =
        unassignIou(daId, acmeId, contractId)

      // save the offsets to test acs snapshots
      offsetP1AfterUnassign = participant1.ledger_api.state.end()
      offsetP2AfterUnassign = participant2.ledger_api.state.end()

      // Make sure the unassignment is observed in acs commitments on daId, and all txns with earlier offsets are
      // observed in acs commitments on both synchronizers
      val wait = reconciliationInterval
      val waitx2 = wait.multipliedBy(2)

      val baseTime0 = clock.now
      val newTime0 = baseTime0.add(waitx2)
      clock.advanceTo(newTime0)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId.toPhysical, newTime0, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId.toPhysical, newTime0, 5.seconds)

      participant1.health.ping(participantId = participant2, synchronizerId = Some(daId))
      participant1.health.ping(participantId = participant2, synchronizerId = Some(acmeId))
      participant2.health.ping(participantId = participant1, synchronizerId = Some(daId))
      participant2.health.ping(participantId = participant1, synchronizerId = Some(acmeId))

      // Wait for the pruning timeout
      val baseTime1 = clock.now
      val newTime1 = baseTime1.add(pruningTimeout)
      clock.advanceTo(newTime1)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      // Prevent pruning of the incomplete reassignment. i.e., check that unassignOffset is not safe to prune
      ensureOffsetUnsafeToPrune(unassignOffsetP1, clock, participant1)
      ensureOffsetUnsafeToPrune(unassignOffsetP2, clock, participant2)

      // Complete the unassignment by submitting the assign from daId to acmeId
      val (assignOffsetP1, assignOffsetP2) = assignIou(daId, acmeId, unassignmentId)

      // save the offsets to test acs snapshots later
      offsetP1AfterAssign = participant1.ledger_api.state.end()
      offsetP2AfterAssign = participant2.ledger_api.state.end()

      // Make sure the assignment is observed in acs commitments on acmeId, and all txns with earlier offsets are
      // observed in acs commitments on both synchronizers
      val baseTime2 = clock.now
      val newTime2 = baseTime2.add(waitx2)
      clock.advanceTo(newTime2)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId.toPhysical, newTime2, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId.toPhysical, newTime2, 5.seconds)

      participant1.health.ping(participantId = participant2, synchronizerId = Some(daId))
      participant1.health.ping(participantId = participant2, synchronizerId = Some(acmeId))
      participant2.health.ping(participantId = participant1, synchronizerId = Some(daId))
      participant2.health.ping(participantId = participant1, synchronizerId = Some(acmeId))

      val baseTime3 = clock.now
      val newTime3 = baseTime3.add(pruningTimeout)
      clock.advanceTo(newTime3)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId.toPhysical, newTime3, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId.toPhysical, newTime3, 5.seconds)

      // Check that the reassignment offset is safe to prune
      ensureOffsetSafeToPrune(assignOffsetP1, clock, participant1)
      ensureOffsetSafeToPrune(assignOffsetP2, clock, participant2)
    }

    "fail to obtain ACS snapshot at offset between the unassign and assign, after having pruned the reassignment after the assign" in {
      implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.state.acs
            .of_party(alice, activeAtOffsetO = Some(offsetP1AfterUnassign)),
          logEntry =>
            logEntry.errorMessage should include(
              "GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED"
            ),
        )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.ledger_api.state.acs
            .of_party(bank, activeAtOffsetO = Some(offsetP2AfterUnassign)),
          logEntry =>
            logEntry.errorMessage should include(
              "GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED"
            ),
        )
    }

    "ACS snapshot succeeds at ledger end after pruning reassignments" in { implicit env =>
      import env.*
      participant1.ledger_api.state.acs
        .of_party(alice, activeAtOffsetO = Some(offsetP1AfterAssign))
      participant2.ledger_api.state.acs
        .of_party(bank, activeAtOffsetO = Some(offsetP2AfterAssign))
    }
  }
}

class ReassignmentPruningIntegrationTestPostgres extends ReassignmentPruningIntegrationTest {
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

//class ReassignmentPruningIntegrationTestH2 extends ReassignmentPruningIntegrationTest {
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

final class ReassignmentPruningIntegrationTestInMemory extends ReassignmentPruningIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](
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
