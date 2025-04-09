// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.util.DamlPackageLoader
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference

trait AcsMultiHostedPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  lazy val darPaths: Seq[String] = Seq(CantonExamplesPath)
  lazy val Packages: Map[PackageId, Ast.Package] = DamlPackageLoader
    .getPackagesFromDarFiles(darPaths)
    .value

  private val iouContract = new AtomicReference[Iou.Contract]
  private val interval = JDuration.ofSeconds(5)

  private lazy val maxDedupDuration = java.time.Duration.ofSeconds(1)

  private val ownerName = "owner"
  private val payerName = "payer"

  private val observer1Name = "observer1"
  private val observer2Name = "observer2"

  private var owner: PartyId = _
  private var payer: PartyId = _
  // we mulithost the observers
  private var observer1: PartyId = _
  private var observer2: PartyId = _

  // for usage while participant is down
  private var participant4IdHolder: ParticipantId = _
  private var participant2IdHolder: ParticipantId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
        }

        sequencer1.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedsynchronizer) =>
          initializedsynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedsynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        // Allocate parties
        PartiesAllocator(Set(participant1, participant2, participant3, participant4))(
          newParties = Seq(
            ownerName -> participant1,
            payerName -> participant1,
            observer1Name -> participant1,
            observer2Name -> participant1,
          ),
          targetTopology = Map(
            ownerName -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
            ),
            payerName -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
            ),
            observer1Name -> Map(
              daId -> (PositiveInt.three, Set(
                (participant1.id, ParticipantPermission.Observation),
                (participant2.id, ParticipantPermission.Observation),
                (participant3.id, ParticipantPermission.Observation),
                (participant4.id, ParticipantPermission.Observation),
              ))
            ),
            observer2Name -> Map(
              daId -> (PositiveInt.two, Set(
                (participant1.id, ParticipantPermission.Observation),
                (participant2.id, ParticipantPermission.Observation),
              ))
            ),
          ),
        )
        owner = ownerName.toPartyId(participant1)
        payer = payerName.toPartyId(participant1)
        observer1 = observer1Name.toPartyId(participant1)
        observer2 = observer2Name.toPartyId(participant1)

        darPaths.foreach { darPath =>
          participants.all.foreach(p => p.dars.upload(darPath))
        }

      }

  private def deployAndCheckContract(
      participant: ParticipantReference,
      owner: PartyId,
      payer: PartyId,
      observers: List[PartyId],
  )(implicit
      env: TestConsoleEnvironment
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract")
    val iou = IouSyntax
      .createIou(participant, Some(daId))(owner, payer, observers = observers)

    iouContract.set(iou)

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      participants.all.foreach(p =>
        p.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      )
    }

    iou
  }

  private def ensureAllRunningParticipantsAreUpToDate()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    // we only use local participants in this test
    val activeParticipants = participants.local.filter(_.health.is_running())
    val activeParticipantIds = activeParticipants.map(_.id)
    eventually() {
      activeParticipants.foreach(p =>
        p.commitments
          .outstanding(
            daName,
            CantonTimestamp.MinValue.toInstant,
            CantonTimestamp.MaxValue.toInstant,
          )
          .filter { case (_, counterP, _) => activeParticipantIds.contains(counterP) }
          shouldBe empty
      )
    }
  }

  private def incrementIntervals(intervalCount: Int)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    ensureAllRunningParticipantsAreUpToDate()
    // we only use local participants in this test
    val activeParticipants = participants.local.filter(_.health.is_running())
    val simClock = environment.simClock.value
    logger.info(
      s"we increase time with a ping $intervalCount times, to force $intervalCount interval generations"
    )
    val start = simClock.now.plusMillis(1)

    for (_ <- 1 to intervalCount) {
      simClock.advance(interval)
      participant1.health.ping(participant1)
      activeParticipants.foreach(_.testing.fetch_synchronizer_times())
    }
    val end = simClock.now

    eventually() {
      activeParticipants.foreach { p =>
        val computed = p.commitments
          .computed(daName, start.toInstant, end.toInstant)

        // we check the total computed amount matches the amount of recipients times intervals
        computed.size shouldBe intervalCount * (participants.all.size - 1)

        // we check the end of time computed amount matches the amount of recipients
        computed.count(
          _._1.toInclusive.forgetRefinement == end
        ) shouldBe (participants.all.size - 1)
      }

      activeParticipants.foreach { p =>
        val received = p.commitments
          .received(daName, start.toInstant, end.toInstant)

        // we check the total received amount matches the amount of senders times intervals
        received.size shouldBe intervalCount * (activeParticipants.size - 1)

        // we check the end of time received amount matches the amount of senders
        received.count(
          _.message.period.toInclusive.forgetRefinement == end
        ) shouldBe (activeParticipants.size - 1)
      }
    }
  }

  private def incrementAndValidate(increments: Int, shouldPruningIncrease: Boolean)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val initialOffset = participant1.pruning.find_safe_offset()

    incrementIntervals(increments)

    eventually() {
      val afterIncrement = participant1.pruning.find_safe_offset()

      if (shouldPruningIncrease)
        afterIncrement should be > initialOffset
      else
        afterIncrement shouldBe initialOffset
    }
  }

  "multi hosted party" when {
    "should prune when time is advancing normally" in { implicit env =>
      import env.*

      deployAndCheckContract(participant1, owner.toLf, payer.toLf, List(observer1, observer2))

      incrementAndValidate(1, shouldPruningIncrease = true)
    }
    "should prune when below BFT threshold participants is down" in { implicit env =>
      import env.*
      participant4IdHolder = participant4.id
      participant4.stop()

      eventually() {
        participant4.health.is_running() shouldBe false
      }

      incrementAndValidate(1, shouldPruningIncrease = true)
    }

    "should prune when the down participant is added to the no-wait configuration" in {
      implicit env =>
        import env.*
        logger.info("participant 4 is still down")
        participant1.commitments.set_no_wait_commitments_from(Seq(participant4IdHolder), Seq(daId))

        incrementAndValidate(1, shouldPruningIncrease = true)
    }

    "should not prune when participant4 is the only not on no wait" in { implicit env =>
      import env.*
      logger.info("participant 4 is still down")
      participant1.commitments.set_wait_commitments_from(Seq(participant4IdHolder), Seq(daId))

      participant1.commitments.set_no_wait_commitments_from(
        Seq(participant3, participant2),
        Seq(daId),
      )

      // our no wait list contains participant2 and participant3.
      // since our wait list only contains participant4, but it is down, then we can not prune.
      // because we need to wait for min (threshold - 1, size(participants-self-noWait)=1) = 1 commitment
      incrementAndValidate(1, shouldPruningIncrease = false)
    }

    "should prune correctly once waiting is re-introduced wait for participant2" in {
      implicit env =>
        import env.*
        // we need to wait for min (threshold - 1, size(participants-self-noWait)=1) = 1 commitment
        // since we wait for participant2, then both observers get a threshold of 2.
        // We can use participant2 to clear the period, even if participant4 is still down.
        participant1.commitments.set_no_wait_commitments_from(Seq(participant4IdHolder), Seq(daId))
        participant1.commitments.set_wait_commitments_from(Seq(participant2), Seq(daId))

        incrementAndValidate(1, shouldPruningIncrease = true)
    }

    "should not prune when more than bft threshold participants are down" in { implicit env =>
      import env.*
      logger.info("participant 4 is still down")
      participant1.commitments.set_wait_commitments_from(Seq(participant3), Seq(daId))
      participant3.stop()

      eventually() {
        participant3.health.is_running() shouldBe false
      }

      // observer1 now has a threshold of 3, but participant3 and participant4 is down, so we can't advance

      incrementAndValidate(1, shouldPruningIncrease = false)
    }

    "should not prune when when threshold is 2 and only counter-participant is down" in {
      implicit env =>
        import env.*
        participant3.start()
        participant3.synchronizers.connect_local(sequencer1, alias = daName)
        participant4.start()
        participant4.synchronizers.connect_local(sequencer1, alias = daName)
        participant2IdHolder = participant2.id
        participant2.stop()

        eventually() {
          participant2.health.is_running() shouldBe false
          participant3.synchronizers.is_connected(daId) shouldBe true
          participant4.synchronizers.is_connected(daId) shouldBe true

          participant1.commitments.outstanding(
            daName,
            CantonTimestamp.MinValue.toInstant,
            CantonTimestamp.MaxValue.toInstant,
          ) shouldBe empty
        }

        // observer2 cannot fulfil its threshold if participant2 is down
        incrementAndValidate(1, shouldPruningIncrease = false)
    }

    "should prune when all counter-participants are in no-wait" in { implicit env =>
      import env.*

      participant1.commitments.set_no_wait_commitments_from(
        Seq(participant3, participant4, participant2IdHolder),
        Seq(daId),
      )
      participant3.stop()
      participant4.stop()

      eventually() {
        participant3.health.is_running() shouldBe false
        participant4.health.is_running() shouldBe false
      }

      incrementAndValidate(1, shouldPruningIncrease = true)
    }

  }

}

//we don't run an in-memory version of this test since it relies on stopping and starting participants.
class AcsMultiHostedPartyIntegrationTestPostgres extends AcsMultiHostedPartyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
