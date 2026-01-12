// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeProportion}
import com.digitalasset.canton.config.{CommitmentSendDelay, DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.util.{CommitmentTestUtil, IntervalDuration}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.pruning.*
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.{
  CommitmentsMismatch,
  NoSharedContracts,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceSynchronizerDisabledUs,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.MemberAccessDisabled
import com.digitalasset.canton.sequencing.protocol.{MemberRecipient, SubmissionRequest}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

sealed trait AcsCommitmentProcessorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with HasProgrammableSequencer
    with CommitmentTestUtil {

  private val iouContract = new AtomicReference[Iou.Contract]
  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val duration: IntervalDuration = IntervalDuration(interval)
  private val minObservationDuration1 = NonNegativeFiniteDuration.tryOfHours(1)
  // Participant2 has a longer minObservationDuration than participant1
  private val minObservationDuration2 = minObservationDuration1 * NonNegativeInt.tryCreate(2)

  private var alreadyDeployedContracts: Seq[Iou.Contract] = Seq.empty

  private var lastCommTick: CantonTimestampSecond = _

  private lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay)
          .replace(
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

        def connect(
            participant: ParticipantReference,
            minObservationDuration: NonNegativeFiniteDuration,
        ): Unit = {
          val daSequencerConnection =
            SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              synchronizerAlias = daName,
              sequencerConnections = daSequencerConnection,
              timeTracker = SynchronizerTimeTrackerConfig(minObservationDuration =
                minObservationDuration.toConfig
              ),
            )
          )
        }

        connect(participant1, minObservationDuration1)
        connect(participant2, minObservationDuration2)
        connect(participant3, minObservationDuration2)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        passTopologyRegistrationTimeout(env)
      }

  "Participants can compute and receive ACS commitments" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value
    deployOnTwoParticipantsAndCheckContract(daId, iouContract, participant1, participant2)

    val tick1 = tickAfter(simClock.uniqueTime())
    val tick2 = tickAfter(tick1.forgetRefinement)
    simClock.advanceTo(tick2.forgetRefinement.immediateSuccessor)

    simClock.advance(interval.plus(JDuration.ofSeconds(1)))
    eventually() {
      participants.local.foreach(_.testing.fetch_synchronizer_times())

      // Make the search for commitments exclusive on both ends, because it's inclusive by default on the end
      participant2.commitments
        .computed(
          daName,
          tick2.toInstant,
          simClock.now.toInstant,
          Some(participant1),
        ) should not be empty
      participant2.commitments
        .received(
          daName,
          tick2.toInstant,
          simClock.now.toInstant,
          Some(participant1),
        ) should not be empty
    }
  }

  "No commitments without an ACS change or time proof" in { implicit env =>
    import env.*

    // No time proofs happen in this test

    val simClock = environment.simClock.value

    logger.info(s"Wait until all commitments were exchanged before starting the next step")
    val epoch = CantonTimestamp.Epoch.toInstant
    val lastTick = tickBeforeOrAt(simClock.now).toInstant
    eventually() {
      val receivedTs = participant1.commitments
        .received(daName, epoch, lastTick, Some(participant2))
        .map(_.message.period.toInclusive)
        .maxOption

      val computedTs = participant2.commitments
        .computed(daName, epoch, lastTick, Some(participant1))
        .map { case (period, _, _) => period.toInclusive }
        .maxOption
      computedTs.map(_.toInstant) should contain(lastTick)
      receivedTs shouldBe computedTs
    }

    eventually() {
      val receivedTs = participant2.commitments
        .received(daName, epoch, lastTick, Some(participant1))
        .map(_.message.period.toInclusive)
        .maxOption

      val computedTs = participant1.commitments
        .computed(daName, epoch, lastTick, Some(participant2))
        .map { case (period, _, _) => period.toInclusive }
        .maxOption
      computedTs.map(_.toInstant) should contain(lastTick)
      receivedTs shouldBe computedTs
    }

    val tickNoCommitments1 = tickAfter(simClock.uniqueTime())
    simClock.advanceTo(tickNoCommitments1.forgetRefinement.plus(interval.multipliedBy(3)))
    logger.info(s"Upload a package to trigger some vetting transactions")
    participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
    participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)
    simClock.advance(interval.plus(JDuration.ofSeconds(1)))

    val start = tickNoCommitments1.forgetRefinement.toInstant
    val end = simClock.now.toInstant
    logger.info(s"Make sure that no commitments are sent or received due to the package upload")
    always(durationOfSuccess = 5.seconds) {
      participant1.commitments.computed(daName, start, end, Some(participant2)) shouldBe empty
      participant1.commitments.received(daName, start, end, Some(participant2)) shouldBe empty
    }

    passTopologyRegistrationTimeout(env)
  }

  "ACS commitments trigger counter-commitments" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value
    val periodBegin = tickAfter(simClock.now)
    val periodEnd = periodBegin.forgetRefinement.add(interval)
    simClock.advanceTo(periodEnd)

    // Participant1 pings itself, which triggers an ACS change
    // So participant1 will create a commitment for its counterparticpants including participant2
    // because there's still a shared active contract between them.
    // So participant2 should also create an ACS commitment and send it back
    // even if participant2 doesn't observe an ACS change
    participant1.health.ping(participant1)

    val start = periodBegin.forgetRefinement.toInstant
    val end = periodEnd.toInstant
    eventually() {
      participant2.commitments.received(
        daName,
        start,
        end,
        Some(participant1),
      ) should not be empty
      participant1.commitments.received(
        daName,
        start,
        end,
        Some(participant2),
      ) should not be empty
    }
  }

  // Starting state assumption: Both participants have the iou contract in their ACS
  // We purge the contract on participant2, therefore participant2 with an empty ACS would not send commitments
  // However, participant1 still has the contract in the ACS, and sends a commitment to participant2
  // Upon receiving this commitment, participant2 sends an empty ACS commitment back to participant1
  "ACS Commitment should trigger counter-commitment even if ACS is empty" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    logger.info(s"ensure participant1 and participant2 is aligned before purging")
    val start = tickAfter(simClock.now)
    simClock.advanceTo(start.forgetRefinement)
    participant1.health.ping(participant1)
    eventually() {
      participant1.commitments
        .lastComputedAndSent(daName)
        .map(t => t.forgetRefinement shouldBe start.forgetRefinement)
      participant2.commitments
        .lastComputedAndSent(daName)
        .map(t => t.forgetRefinement shouldBe start.forgetRefinement)
    }

    logger.info(s"Purging contract from participant2")
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        logger.info(
          s"""Disconnect both participants from the synchronizer. We disconnect participant1 because we don't
            want it to observe time advancing, and participant2 because we want to purge a contract."""
        )
        Seq(participant1, participant2).foreach(p => p.synchronizers.disconnect_all())
        participant2.repair.purge(daName, Seq(iouContract.get().id.toLf))

        logger.info(
          s"Advance the time on the synchronizer past the contract purge time"
        )

        val divergence = tickAfter(start.forgetRefinement)
        simClock.advanceTo(divergence.forgetRefinement)

        logger.info(
          s"Reconnect participant2 and wait until the contract is purged on participant2"
        )
        participant2.synchronizers.reconnect_local(daName)
        eventually() {
          participant2.synchronizers.list_connected() should not be empty
          participant2.ledger_api.state.acs.of_all() shouldBe empty
        }

        logger.info(
          "Make sure participant2 observed the time advance through the acs commitment processor so that it does not buffer incoming commitments"
        )
        participant2.health.ping(participant2)
        eventually() {
          participant2.commitments
            .lastComputedAndSent(daName)
            .map(t => t.forgetRefinement shouldBe simClock.now)
        }

        logger.info(
          "Next, reconnect participant1 and advance the time on participant1 so that it sends a commitment to participant2"
        )
        participant1.synchronizers.reconnect_local(daName)

        eventually() {
          participant1.synchronizers.list_connected() should not be empty
          participant1.ledger_api.state.acs.of_all() should not be empty
          participant1.commitments
            .lastComputedAndSent(daName)
            .map(t =>
              t.forgetRefinement shouldBe tickBeforeOrAt(
                divergence.forgetRefinement.plusMillis(-1L)
              ).forgetRefinement
            )
        }
        val end = tickAfter(divergence.forgetRefinement)
        simClock.advanceTo(end.forgetRefinement)

        participant1.health.ping(participant1)

        logger.info(s"Check that participant2 sent back an empty commitment to participant1")
        eventually() {
          participant1.commitments
            .lastComputedAndSent(daName)
            .map(t => t.forgetRefinement shouldBe simClock.now)
          participant1.commitments
            .received(
              daName,
              divergence.toInstant,
              end.toInstant,
              Some(participant2),
            )
            .size should be >= 1

          participant1.commitments
            .received(
              daName,
              divergence.toInstant,
              end.toInstant,
              Some(participant2),
            )
            .map(c => c.message.commitment shouldBe AcsCommitmentProcessor.hashedEmptyCommitment)
        }
      }.asJava,
      logs => {
        forAtLeast(1, logs)(m => m.message should include(CommitmentsMismatch.id))
        forAtLeast(1, logs)(m => m.message should include(NoSharedContracts.id))
      },
    )
  }

  // Starting state assumption: participant1 has the iou contract in its ACS, participant2 has an empty ACS
  // We purge the contract on participant1 as well, therefore both participants have an empty ACS
  // After a reconciliation interval, no participant sends commitments
  // At the end, we redeploy the contracts on both participants for the following tests, and wait for a commitment
  "No Commitment when both ACSes are empty" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        logger.info(
          "Advance the time to the end of a commitment period to deterministically trigger " +
            "the last mismatch, so that we can eventually wait for it"
        )
        lastCommTick = tickAfter(simClock.uniqueTime())
        simClock.advanceTo(lastCommTick.forgetRefinement.immediateSuccessor)
        participants.all.foreach(p => p.testing.fetch_synchronizer_times())

        logger.info(s"We realign the ACSes by removing the Iou from participant1 as well")
        participant1.synchronizers.disconnect_all()
        participant1.repair.purge(daName, Seq(iouContract.get().id.toLf))
        participant1.synchronizers.reconnect_local(daName)

        logger.info(
          s"Wait until the contract is purged on participant1."
        )
        eventually() {
          participant1.ledger_api.state.acs.of_all() shouldBe empty
        }

        logger.info(
          s"Wait to receive the exchanged commitments"
        )

        eventually() {
          participant1.commitments
            .received(
              daName,
              lastCommTick.toInstant.minusMillis(1),
              lastCommTick.toInstant,
              Some(participant2),
            )
            .size shouldBe 1
        }

        eventually() {
          participant2.commitments
            .received(
              daName,
              lastCommTick.toInstant.minusMillis(1),
              lastCommTick.toInstant,
              Some(participant1),
            )
            .size shouldBe 1
        }

        val firstNoCommTick = tickAfter(simClock.uniqueTime())
        val secondNoCommTick = tickAfter(firstNoCommTick.forgetRefinement)

        // Every minObservationDuration, the participant asks for a time proof, so this represents the maximum amount
        // of time that the participant might not have observed. As participant do not do anything else while waiting
        // for this time to elapse, we know that all commitments that were ever sent will have been processed after we
        // advance the time.
        logger.info(
          "After the min observation duration, participant1 should request a time proof and process potentially " +
            "buffered commitments"
        )
        simClock.advance(minObservationDuration1.duration)

        eventually() {
          participants.all.foreach(p =>
            p.testing
              .await_synchronizer_time(
                daId,
                firstNoCommTick.forgetRefinement.immediateSuccessor,
              )
          )
        }

        logger.info(
          s"The ACSes should be empty after time $lastCommTick and no more commitments should be sent, " +
            s"in particular not in between $firstNoCommTick and $secondNoCommTick"
        )

        eventually() {
          participant1.commitments.received(
            daName,
            firstNoCommTick.toInstant,
            secondNoCommTick.toInstant,
            Some(participant2),
          ) shouldBe empty
          participant2.commitments.received(
            daName,
            firstNoCommTick.toInstant,
            secondNoCommTick.toInstant,
            Some(participant1),
          ) shouldBe empty
        }
      },
      logs => {
        // Because we disconnect participant 1 to purge contracts, it can happen that the clean request counter hasn't
        // advanced to after processing the incoming commitment, therefore participant 1 processes it again and
        // issues the mismatch warning again. This should happen at least once and at most twice
        forAtLeast(1, logs) { m =>
          m.toString should (include(s"toInclusive = $lastCommTick") and include(
            "sender = PAR::participant2"
          ) and
            include("counterParticipant = PAR::participant1") and include(
              CommitmentsMismatch.id
            ))
        }
        forExactly(1, logs) { m =>
          m.toString should (include(s"toInclusive = $lastCommTick")
            and include(
              "sender = PAR::participant1"
            ) and
            include("counterParticipant = PAR::participant2")
            and include(
              NoSharedContracts.id
            ))
        }
      },
      timeUntilSuccess = 60.seconds,
    )

    logger.info(s"We deploy the IOU again for following tests.")
    alreadyDeployedContracts = alreadyDeployedContracts.appended(
      deployOnTwoParticipantsAndCheckContract(daId, iouContract, participant1, participant2)
    )
  }

  "Periodic synchronizer time proofs trigger commitment computations" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    val start = tickAfter(simClock.now).forgetRefinement.immediateSuccessor.toInstant

    logger.info(
      s"After the min observation duration, participant1 should request a time proof and compute a new round of commitments"
    )
    simClock.advance(
      minObservationDuration1.duration
        // Allow some margin as the sim clock advancement doesn't take into account the unique timestamps
        // that have already been issued after sim clock "now".
        .plusMillis(1)
    )
    val end = simClock.now.toInstant
    eventually() {
      participant2.commitments.received(
        daName,
        start,
        end,
        Some(participant1),
      ) should not be empty
      participant1.commitments.received(
        daName,
        start,
        end,
        Some(participant2),
      ) should not be empty
    }
  }

  "retry send commitments only to the registered members" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    deployOnTwoParticipantsAndCheckContract(
      daId,
      iouContract,
      participant1,
      participant2,
      observers = Seq(participant3),
    )

    val seq = getProgrammableSequencer(sequencer1.name)
    val p1RevokedP = Promise[Unit]()

    val fromP2ToAll = new AtomicInteger()
    val fromP3ToAll = new AtomicInteger()
    val fromP2ToP3Only = new AtomicInteger()
    val fromP3ToP2Only = new AtomicInteger()

    def checkSequencedMessage(
        submissionRequest: SubmissionRequest,
        expectedSender: ParticipantReference,
        firstReceiver: ParticipantReference,
        otherReceivers: ParticipantReference*
    ): Boolean = {
      val expectedReceivers =
        (firstReceiver +: otherReceivers).map(ref => MemberRecipient(ref.member)).toSet
      submissionRequest.sender == expectedSender.member && submissionRequest.batch.allRecipients == expectedReceivers
    }
    // Hold back all ACS commitment messages until participant 2 pinged participants 1 and 3, so all see time advance,
    // and participant 1 has been revoked
    logger.info("Set policy: wait for revocation of participant 1")
    seq.setPolicy_("delay ACS commitments until revocation of participant 1") { submissionRequest =>
      if (ProgrammableSequencerPolicies.isAcsCommitment(submissionRequest)) {
        if (checkSequencedMessage(submissionRequest, participant2, participant1, participant3))
          fromP2ToAll.getAndIncrement()
        if (checkSequencedMessage(submissionRequest, participant2, participant3))
          fromP2ToP3Only.getAndIncrement()
        if (checkSequencedMessage(submissionRequest, participant3, participant1, participant2))
          fromP3ToAll.getAndIncrement()
        if (checkSequencedMessage(submissionRequest, participant3, participant2))
          fromP3ToP2Only.getAndIncrement()
        SendDecision.HoldBack(p1RevokedP.future)
      } else SendDecision.Process
    }

    val periodBegin = tickAfter(simClock.now)
    simClock.advanceTo(periodBegin.forgetRefinement)

    logger.info("Participant 2 pings participants 1 and 3, so that all see time advance")
    participant2.testing.maybe_bong(
      targets = Set(participant1, participant3),
      validators = Set(participant2),
      levels = 0,
    ) shouldBe defined

    def commitmentStatus()
        : Map[LocalParticipantReference, Set[(LocalParticipantReference, Int, Int)]] = {
      val allParticipants = Set(participant1, participant2, participant3)
      allParticipants
        .map(p =>
          p -> allParticipants.excl(p).map { c =>
            val computed =
              p.commitments.computed(
                daName,
                periodBegin.toInstant.minusMillis(1),
                periodBegin.toInstant,
                Some(c),
              )
            val received =
              p.commitments.received(
                daName,
                periodBegin.toInstant.minusMillis(1),
                periodBegin.toInstant,
                Some(c),
              )
            (c, computed.size, received.size)
          }
        )
        .toMap
    }

    logger.info(
      "Until we disconnect participant 1, no commitments should be received, because the sequencer blocks delivery."
    )
    eventually() {
      commitmentStatus() shouldBe Map(
        participant2 -> Set((participant1, 1, 0), (participant3, 1, 0)),
        participant3 -> Set((participant1, 1, 0), (participant2, 1, 0)),
        participant1 -> Set((participant2, 1, 0), (participant3, 1, 0)),
      )
    }

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        logger.info(
          "Revoke the certificate of participant 1 on da. This means that participant 1 cannot receive commitments."
        )
        participant1.topology.synchronizer_trust_certificates.propose(
          participantId = participant1.id,
          synchronizerId = daId,
          change = TopologyChangeOp.Remove,
        )
        // wait until the topology states on the sequencer, participant 2 and participant 3 see participant1
        // removed from the daId synchronizer
        eventually() {
          sequencer1.topology.participant_synchronizer_states
            .active(daId, participant1.id) shouldBe false
          participant2.topology.participant_synchronizer_states
            .active(daId, participant1.id) shouldBe false
          participant3.topology.participant_synchronizer_states
            .active(daId, participant1.id) shouldBe false
        }

        p1RevokedP.trySuccess(())

        logger.info(
          "Once we are ready to release the stalled commitments on the sequencer, we should see the retry policies kick in."
        )

        logger.info(
          "Participant 2 computes commitments for all, but receives only from participant 3." +
            "Participant 3 computes commitments for all, but receives only from participant 2." +
            "Participant 1 computes commitments for all, but receives none."
        )
        eventually() {
          commitmentStatus() shouldBe Map(
            participant2 -> Set((participant1, 1, 0), (participant3, 1, 1)),
            participant3 -> Set((participant1, 1, 0), (participant2, 1, 1)),
            participant1 -> Set((participant2, 1, 0), (participant3, 1, 0)),
          )

          fromP2ToAll.get() shouldBe 1
          fromP3ToAll.get() shouldBe 1
          fromP2ToP3Only.get() shouldBe 1
          fromP3ToP2Only.get() shouldBe 1
        }

        // When sequencer1 processes the removal of participant1's trust certificate, it will close participant1's
        // subscription. participant1 will try to reconnect, but as sequencer1 answers with `CLIENT_AUTHENTICATION_REJECTED`,
        // participant1 will eventually disconnect from this synchronizer.
        // Wait for this disconnection to happen.
        eventually() {
          participant1.synchronizers.is_connected(
            initializedSynchronizers(daName).synchronizerId
          ) shouldBe false
        }
      },
      forEvery(_) { entry =>
        entry.message should (include(
          MemberAccessDisabled(participant1.id).reason
        ) or include(
          SyncServiceSynchronizerDisabledUs.id
        ) or // The participant might have started to refresh the token before being disabled, but the refresh request
          // is processed by the sequencer after the participant is disabled
          include(
            "Health-check service responded NOT_SERVING for"
          ) or include("Token refresh aborted due to shutdown")
          // the participant might not actually get the dispatched transaction delivered,
          // because the sequencer may cut the participant's connection before delivering the topology broadcast
          or include regex ("Waiting for transaction .* to be observed")
          or include("Unknown recipients: PAR::participant1")
          or include("(Eligible) Senders are unknown: PAR::participant1")
          or include("UNAVAILABLE/Channel shutdownNow invoked"))
      },
    )
  }

  "Removed participant should not trigger commitment computations" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
    eventually() {
      participant1.synchronizers.is_connected(
        initializedSynchronizers(acmeName).synchronizerId
      ) shouldBe true
      participant2.synchronizers.is_connected(
        initializedSynchronizers(acmeName).synchronizerId
      ) shouldBe true
    }

    val cmd =
      new Iou(
        participant2.adminParty.toProtoPrimitive,
        participant3.adminParty.toProtoPrimitive,
        new Amount(3.50.toBigDecimal, "CHF"),
        List().asJava,
      ).create.commands.asScala.toSeq
    participant2.ledger_api.javaapi.commands.submit(
      Seq(participant2.id.adminParty),
      cmd,
      Some(acmeId),
    )
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        logger.debug("Removing participant2 on synchronizer da")
        participant2.topology.synchronizer_trust_certificates.propose(
          participantId = participant2.id,
          synchronizerId = initializedSynchronizers(acmeName).synchronizerId,
          change = TopologyChangeOp.Remove,
        )
        eventually() {
          participant2.synchronizers.is_connected(
            initializedSynchronizers(acmeName).synchronizerId
          ) shouldBe false
        }
        logger.info("Successfully disconnected participant2 from synchronizer acme")

        // Sit out for a full commitment period, so that we can later check that the participants don't exchange
        // messages for this period
        val tickNoCommitments1 = tickAfter(simClock.uniqueTime())
        val tickNoCommitments2 = tickAfter(tickNoCommitments1.forgetRefinement)
        simClock.advanceTo(tickNoCommitments2.forgetRefinement.immediateSuccessor)

        simClock.advance(interval.plus(JDuration.ofSeconds(1)))
        Seq(participant2, participant3).foreach(_.testing.fetch_synchronizer_times())

        val startNoCommitment = tickNoCommitments1.toInstant
        participant2.commitments
          .computed(
            acmeName,
            startNoCommitment,
            Instant.now(),
            Some(participant3),
          ) shouldBe empty
        participant2.commitments
          .received(
            acmeName,
            startNoCommitment,
            Instant.now(),
            Some(participant3),
          ) shouldBe empty
        participant3.commitments
          .received(
            acmeName,
            startNoCommitment,
            Instant.now(),
            Some(participant2),
          ) shouldBe empty
        participant3.commitments
          .computed(
            acmeName,
            startNoCommitment,
            Instant.now(),
            Some(participant1),
          ) shouldBe empty
      },
      forEvery(_) { entry =>
        entry.message should (include(
          MemberAccessDisabled(participant2.id).reason
        ) or include(
          MemberAccessDisabled(participant1.id).reason
        ) or include(
          SyncServiceSynchronizerDisabledUs.id
        ) or // The participant might have started to refresh the token before being disabled, but the refresh request
          // is processed by the sequencer after the participant is disabled
          include(
            "Health-check service responded NOT_SERVING for"
          )
          // Added due to pool not propagating the "disabled us" error
          or include(SyncServiceSynchronizerDisconnect.id))
      },
    )
  }
}

class AcsCommitmentProcessorReferenceIntegrationTestPostgres
    extends AcsCommitmentProcessorIntegrationTest {
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
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class AcsCommitmentProcessorReferenceIntegrationTestH2
//    extends AcsCommitmentProcessorIntegrationTest {
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
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
