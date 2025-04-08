// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.InvalidTrafficPurchasedMessage
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isTopUpBalance
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.Member

import scala.concurrent.Promise
import scala.concurrent.duration.*

trait TrafficControlConcurrentTopologyChangeTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  protected var programmableSequencer1: ProgrammableSequencer = _

  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    enforceRateLimiting = true,
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M2
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            trafficControl = Some(trafficControlParameters),
            // So that topology changes become effective as of sequencing time
            topologyChangeDelay = config.NonNegativeFiniteDuration.Zero,
          ),
        )

        participant1.synchronizers.connect_local(sequencer1, daName)
        participant2.synchronizers.connect_local(sequencer1, daName)

        programmableSequencer1 = getProgrammableSequencer(sequencer1.name)
      }

  "top up all participants and mediators" in { implicit env =>
    import env.*

    val membersToTopUp = participants.local.map(_.id) ++ mediators.local.map(_.id)

    membersToTopUp.foreach { member =>
      sequencer1.traffic_control
        .set_traffic_balance(member, PositiveInt.one, NonNegativeLong.tryCreate(1_000_000))
    }

    // Have the participant ping itself to move the synchronizer time such that the top up for the last member above
    // becomes effective and visible
    participant1.health.ping(participant1)

    eventually() {
      val trafficStatus =
        sequencer1.traffic_control.traffic_state_of_members(membersToTopUp).trafficStates
      trafficStatus
        .collect { case (_, traffic) =>
          traffic.serial
        }
        .toList
        .distinct
        .loneElement shouldBe Some(PositiveInt.one)
    }
  }

  "concurrent top-up request and topology change" should {
    "be resolved by retrying" in { implicit env =>
      import env.*

      // Scenario:
      //
      // - The topology of sequencers is: 2 sequencers, threshold = 1
      // - A top-up request is sent by the sequencers
      // - Before the top-up request is received and validated, the topology changes to use threshold = 2
      // - The top-up request is rejected
      // - The sequencers resend the same top-up request
      // - As the new topology is used both at send time and validation time, the top-up request is now accepted

      val newBalance = NonNegativeLong.tryCreate(2_000_000)

      val delayingP = Promise[Unit]()
      val topUpQueuedP = Promise[Unit]()

      // Delay the top-up delivery until after the topology has changed
      // Note: using `.map()` instead of `.onComplete()` so that exceptions are propagated
      // into the `Future` and into the test via `.futureValue`, which makes it easier to debug.
      val afterChangeThresholdF = topUpQueuedP.future.map { _ =>
        changeThreshold()
        delayingP.trySuccess(())
      }

      programmableSequencer1.setPolicy_("delay top-up delivery")(SendPolicy.processTimeProofs_ {
        submissionRequest =>
          if (isTopUpBalance(submissionRequest)) {
            topUpQueuedP.trySuccess(())
            SendDecision.HoldBack(delayingP.future)
          } else
            SendDecision.Process
      })

      val (serial, _) = getLatestSerialAndBalanceForMember(participant1)
      val newSerial = serial.increment
      newSerial should be > PositiveInt.one

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          sequencer1.traffic_control
            .set_traffic_balance(participant1, newSerial, newBalance)
            .discard

          // The top-up is never observed
          always(durationOfSuccess = 5.seconds) {
            val (serial, balance) = getLatestSerialAndBalanceForMember(participant1)
            serial should be < newSerial
            balance should be < newBalance.value
          }
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficPurchasedMessage,
                _ should include(s"signature threshold not reached"),
              ),
              "rejected top-up",
            )
          )
        ),
      )

      afterChangeThresholdF.futureValue

      programmableSequencer1.resetPolicy()

      // Re-send -- the change of threshold implies a different aggregation ID.
      // If the time bucket has changed since the first try, it will also contribute to change the aggregation ID.
      Seq(sequencer1, sequencer2).foreach { sequencer =>
        sequencer.traffic_control.set_traffic_balance(participant1, newSerial, newBalance).discard
      }

      // Have the participant ping itself to move the synchronizer time such that the top up for the last member above
      // becomes effective and visible
      participant1.health.ping(participant1)

      eventually() {
        // The top-up is observed
        val (serial, balance) = getLatestSerialAndBalanceForMember(participant1)
        serial shouldBe newSerial
        balance shouldBe newBalance.value
      }
    }
  }

  private def getLatestSerialAndBalanceForMember(
      member: Member
  )(implicit env: TestConsoleEnvironment): (PositiveInt, Long) = {
    import env.*

    val trafficStatus = sequencer1.traffic_control
      .traffic_state_of_members(Seq(member))
      .trafficStates
      .get(member)
      .value

    val serial = trafficStatus.serial.value
    val balance = trafficStatus.extraTrafficRemainder

    (serial, balance)
  }

  private def changeThreshold()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    // Update the sequencer topology to have threshold = 2
    val synchronizerOwners = initializedSynchronizers(daName).synchronizerOwners

    synchronizerOwners.foreach { owner =>
      owner.topology.sequencers.propose(
        synchronizerId = daId,
        threshold = PositiveInt.two,
        active = Seq(sequencer1.id, sequencer2.id),
        mustFullyAuthorize = true,
      )
    }
  }
}

class TrafficControlConcurrentTopologyChangeTestPostgres
    extends TrafficControlConcurrentTopologyChangeTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
