// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection.assertInAcsSync
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.grpc.InspectionServiceError
import com.digitalasset.canton.participant.pruning.*
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.{
  CommitmentsMismatch,
  NoSharedContracts,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceSynchronizerDisabledUs
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.MemberAccessDisabled
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.{HasExecutionContext, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*

sealed trait AcsCommitmentProcessorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with HasExecutionContext {

  private val iouContract = new AtomicReference[Iou.Contract]
  private val interval = JDuration.ofSeconds(5)
  private val minObservationDuration1 = NonNegativeFiniteDuration.tryOfHours(1)
  // Participant2 has a longer minObservationDuration than participant1
  private val minObservationDuration2 = minObservationDuration1 * NonNegativeInt.tryCreate(2)

  private var alreadyDeployedContracts: Seq[Iou.Contract] = Seq.empty

  private var lastCommTick: CantonTimestampSecond = _

  private lazy val maxDedupDuration = java.time.Duration.ofHours(1)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfHours(1)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfHours(1)
  private val pruningTimeout =
    Ordering[JDuration].max(
      interval
        .plus(confirmationResponseTimeout.duration)
        .plus(mediatorReactionTimeout.duration),
      maxDedupDuration,
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
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

        connect(participant1, minObservationDuration1)
        connect(participant2, minObservationDuration2)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.foreach(_.dars.upload(CantonExamplesPath))
        passTopologyRegistrationTimeout(env)
      }

  // advance the time sufficiently past the topology transaction registration timeout,
  // so that the ticks to detect a timeout for topology submissions does not seep into the following tests
  private def passTopologyRegistrationTimeout(environment: TestConsoleEnvironment): Unit =
    environment.environment.simClock.value.advance(
      (environment.participant1.config.topology.topologyTransactionRegistrationTimeout).asFiniteApproximation.toJava
    )
  private def deployAndCheckContract(synchronizerId: SynchronizerId)(implicit
      env: TestConsoleEnvironment
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract on both participants")
    val iou = IouSyntax
      .createIou(participant1, Some(synchronizerId))(
        participant1.adminParty,
        participant2.adminParty,
      )

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

  private def tickBeforeOrAt(timestamp: CantonTimestamp): CantonTimestampSecond =
    SortedReconciliationIntervals
      .create(
        Seq(mkParameters(CantonTimestamp.MinValue, interval.getSeconds)),
        CantonTimestamp.MaxValue,
      )
      .value
      .tickBeforeOrAt(timestamp)
      .value

  private def tickAfter(timestamp: CantonTimestamp): CantonTimestampSecond =
    tickBeforeOrAt(timestamp) + PositiveSeconds.tryOfSeconds(interval.getSeconds)

  "Participants can compute and receive ACS commitments" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value
    deployAndCheckContract(daId)

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
    participant1.dars.upload(CantonTestsPath)
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
        participants.all.foreach(p => p.synchronizers.disconnect_all())
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
              lastCommTick.toInstant,
              lastCommTick.toInstant,
              Some(participant2),
            )
            .size shouldBe 1
        }

        eventually() {
          participant2.commitments
            .received(
              daName,
              lastCommTick.toInstant,
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
                daId.toPhysical,
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
        forExactly(1, logs) { m =>
          m.toString should (include(s"toInclusive = $lastCommTick") and include(
            CommitmentsMismatch.id
          ))
        }
        forExactly(1, logs) { m =>
          m.toString should (include(s"toInclusive = $lastCommTick") and include(
            NoSharedContracts.id
          ))
        }
      },
      timeUntilSuccess = 60.seconds,
    )

    logger.info(s"We deploy the IOU again for following tests.")
    alreadyDeployedContracts = alreadyDeployedContracts.appended(deployAndCheckContract(daId))
  }

  "Periodic synchronizer time proofs trigger commitment computations" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    val start = tickAfter(simClock.now).forgetRefinement.immediateSuccessor.toInstant

    logger.info(
      s"After the min observation duration, participant1 should request a time proof and compute a new round of commitments"
    )
    simClock.advance(minObservationDuration1.duration)
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

  "Commitment and mismatch inspection" should {

    def deployThreeAndCheck(synchronizerId: SynchronizerId)(implicit
        env: FixtureParam
    ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) = {
      import env.*

      val simClock = environment.simClock.value

      val c1 = deployAndCheckContract(synchronizerId)
      val c2 = deployAndCheckContract(synchronizerId)
      val c3 = deployAndCheckContract(synchronizerId)
      val createdCids = Seq(c1, c2, c3)

      val tick1 = tickAfter(simClock.uniqueTime())
      simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
      participant1.testing.fetch_synchronizer_times()

      val p1Computed = eventually() {
        val p1Computed = participant1.commitments
          .computed(
            daName,
            tick1.toInstant,
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

    "participant can open a commitment it previously sent" in { implicit env =>
      import env.*

      val (createdCids, period, commitment) = deployThreeAndCheck(daId)

      val contractsAndTransferCounters = participant1.commitments.open_commitment(
        commitment,
        daId,
        period.toInclusive.forgetRefinement,
        participant2,
      )

      val returnedCids = contractsAndTransferCounters.map(c => c.cid)
      returnedCids should contain theSameElementsAs alreadyDeployedContracts.map(c => c.id.toLf)
    }

    "participant can open a commitment spanning multiple intervals" in { implicit env =>
      import env.*

      val simClock = environment.simClock.value

      deployThreeAndCheck(daId)

      logger.info(
        "Advance time five reconciliation intervals, remembering the tick after three reconciliation intervals."
      )
      simClock.advance(interval.multipliedBy(3))
      val midTick = tickAfter(simClock.uniqueTime())
      simClock.advanceTo(midTick.forgetRefinement.immediateSuccessor)
      simClock.advance(interval.multipliedBy(2))
      val cmtTick = tickAfter(simClock.uniqueTime())
      simClock.advanceTo(cmtTick.forgetRefinement.immediateSuccessor)

      logger.info("Send a commitment, which will cover the five intervals.")
      participant1.testing.fetch_synchronizer_times()

      val (_period, _pId, cmtOverFiveIntervals) = eventually() {
        val p1Computed = participant1.commitments
          .computed(
            daName,
            cmtTick.toInstant,
            cmtTick.toInstant,
            Some(participant2),
          )
        p1Computed.size shouldBe 1
        p1Computed
      }.loneElement

      val contractsAndTransferCounters = participant1.commitments.open_commitment(
        cmtOverFiveIntervals,
        daId,
        midTick.forgetRefinement,
        participant2,
      )

      val returnedCids = contractsAndTransferCounters.map(c => c.cid)
      returnedCids should contain theSameElementsAs alreadyDeployedContracts.map(c => c.id.toLf)
    }

    "opening a commitment fails when" should {
      "the participant didn't send the given commitment" in { implicit env =>
        import env.*

        val (_createdCids, period, commitment) = deployThreeAndCheck(daId)
        val notSentCmt = LtHash16().getByteString()
        val hashedNotSentCmd = AcsCommitment.hashCommitment(notSentCmt)

        // give wrong commitment but correct timestamp and counter-participant
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            hashedNotSentCmd,
            daId,
            period.toInclusive.forgetRefinement,
            participant2,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
            ))
            logEntry.shouldBeCantonErrorCode(InspectionServiceError.IllegalArgumentError)
          },
        )
      }

      "the participant sent the commitment but not at the given tick" in { implicit env =>
        import env.*

        val (_createdCids, period, commitment) = deployThreeAndCheck(daId)

        // give wrong timestamp but a computed commitment and correct counter-participant
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            commitment,
            daId,
            CantonTimestamp.MinValue,
            participant2,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
            ))
            logEntry.shouldBeCantonErrorCode(InspectionServiceError.IllegalArgumentError)
          },
        )
      }

      "the given counter-participant is incorrect" in { implicit env =>
        import env.*

        val (_createdCids, period, commitment) = deployThreeAndCheck(daId)

        // give wrong counter-participant but a computed commitment and its correct timestamp
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            commitment,
            daId,
            period.toInclusive.forgetRefinement,
            participant1,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
            ))
            logEntry.shouldBeCantonErrorCode(InspectionServiceError.IllegalArgumentError)
          },
        )
      }

      "the given timestamp is not a reconciliation interval tick" in { implicit env =>
        import env.*

        val (_createdCids, period, commitment) = deployThreeAndCheck(daId)

        // this test assumes that the reconciliation interval is not 1 second for the given opening commitment
        // timestamp to not fall on a reconciliation interval boundary
        assert(interval.getSeconds != 1)

        // give timestamp that does not correspond to a reconciliation interval boundary
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            commitment,
            daId,
            // this test assumes that the reconciliation interval is not 1 second for the timestamp below to not fall
            // on an interval reconciliation boundary
            period.toInclusive.forgetRefinement.minus(JDuration.ofSeconds(1)),
            participant1,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "is not a valid reconciliation interval tick"
            ))
            logEntry.shouldBeCantonErrorCode(InspectionServiceError.IllegalArgumentError)
          },
        )
      }
    }

    "acs pruning beyond the timestamp prevents opening a commitment" in { implicit env =>
      import env.*
      import cats.syntax.either.*

      val simClock = environment.simClock.value

      val (_createdCids, period, commitment) = deployThreeAndCheck(daId)

      logger.info(
        "Participant1 waits to receive counter-commitment, so that it can prune past data"
      )
      eventually() {
        participant1.commitments
          .received(
            daName,
            period.toInclusive.toInstant,
            period.toInclusive.toInstant,
            Some(participant2),
          )
          .size shouldBe 1
      }

      simClock.advance(pruningTimeout)
      logger.info(
        "Participant1 deploy some more contracts to advance the clean replay, so that it can prune past data"
      )
      deployThreeAndCheck(daId)

      logger.info("Wait that ACS background pruning advanced past the timestamp of the commitment")
      eventually() {
        val pruningTs = participant1.testing.state_inspection.acsPruningStatus(daName)
        pruningTs.map(_.lastSuccess.forall(_ >= period.toInclusive)) shouldBe Some(true)
      }

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Either
          .catchOnly[CommandFailure] {
            participant1.commitments.open_commitment(
              commitment,
              daId,
              period.toInclusive.forgetRefinement,
              participant2,
            )
          }
          .left
          .value
          .getMessage should include("Command execution failed"),
        logs => {
          forExactly(1, logs)(m =>
            m.message should (include(
              s"Active contract store for synchronizer"
            ) and include(
              "which is after the requested time of change"
            ))
          )
        },
      )
    }

    "inspect commitment contracts" should {
      def getCleanReqTs(
          participant: LocalParticipantReference,
          synchronizerId: SynchronizerId,
      ): Option[CantonTimestamp] = {
        val cleanReqTs = eventually() {
          participant.underlying.value.sync.participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerId)
            .futureValueUS
            .flatMap(_.sequencerIndex)
        }.map(_.sequencerTimestamp)
        cleanReqTs
      }

      "inspect created and archived contracts at the current timestamp" in { implicit env =>
        import env.*

        logger.info("Create three contracts on synchronizer da")
        val (createdCidsDa, _per, _cmt) = deployThreeAndCheck(daId)

        // archive one of these created contracts
        logger.info("Archive one of these contaracts")
        val archivedCid = createdCidsDa.headOption.getOrElse(fail("No created contract found")).id
        participant1.ledger_api.javaapi.commands.submit(
          Seq(participant1.id.adminParty),
          archivedCid.exerciseArchive().commands.asScala.toSeq,
          Some(daId),
        )

        eventually() {
          participants.all.foreach(p =>
            p.ledger_api.state.acs.of_all().map(_.contractId) should not contain archivedCid
          )
        }

        val tsAfterArchival =
          getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

        val queriedCids = createdCidsDa.map(_.id.toLf)
        val inspectContracts = participant1.commitments.inspect_commitment_contracts(
          queriedCids,
          tsAfterArchival,
          daId,
          downloadPayload = true,
        )

        logger.info(
          s"Inspect the contract state after archival timestamp $tsAfterArchival valid on synchronizer da"
        )

        logger.info(s"The result contains exactly one entry per contract")
        inspectContracts.size shouldBe queriedCids.size
        inspectContracts.map(_.cid) should contain theSameElementsAs queriedCids

        logger.info(s"The result contains a payload for all contracts")
        inspectContracts.map(
          _.contract.map(_.contractId)
        ) should contain theSameElementsAs queriedCids.map(c => Some(c))

        logger.info(s"Non-archived contracts have a single state (created) and are active on da")
        inspectContracts.filter { e =>
          e.activeOnExpectedSynchronizer &&
          e.state.sizeIs == 1 && e.state.forall(_.contractState.isInstanceOf[ContractCreated])
        } should have size (queriedCids.size - 1).toLong

        logger.info(
          s"The archived contract has two states (created and archived) and is not active on da"
        )
        inspectContracts.filter { e =>
          e.cid == archivedCid.toLf && !e.activeOnExpectedSynchronizer &&
          e.state.sizeIs == 2 && e.state.count(
            _.contractState.isInstanceOf[ContractArchived]
          ) == 1 && e.state.count(
            _.contractState.isInstanceOf[ContractCreated]
          ) == 1
        } should have size 1
      }

      "do not retrieve payloads works" in { implicit env =>
        import env.*

        logger.info("Create three contracts on synchronizer da")
        val (createdCidsDa, _per, _cmt) = deployThreeAndCheck(daId)

        val ts =
          getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

        val queriedCids = createdCidsDa.map(_.id.toLf)
        val inspectContracts = participant1.commitments.inspect_commitment_contracts(
          queriedCids,
          ts,
          daId,
          downloadPayload = false,
        )

        logger.info(
          s"Inspect the contract state after creation timestamp $ts valid on synchronizer da"
        )

        logger.info(s"The result contains exactly one entry per contract")
        inspectContracts.size shouldBe queriedCids.size
        inspectContracts.map(_.cid) should contain theSameElementsAs queriedCids

        logger.info(s"The result does not contains a payload for any contracts")
        forAll(inspectContracts)(inspectContract => inspectContract.contract shouldBe None)
      }

      "inspect contract state for a past unpruned timestamp and more synchronizers" in {
        implicit env =>
          import env.*

          participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

          logger.info("Create three contracts on synchronizer da")
          val (createdCidsDa, _per, _cmt) = deployThreeAndCheck(daId)

          val tsBeforeReassign =
            getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

          logger.info("Reassign one contract from da to acme")
          val reassignedCid =
            createdCidsDa.headOption.getOrElse(fail("No created contract found")).id.toLf
          val unassignedWrapper =
            participant1.ledger_api.commands
              .submit_unassign(
                participant1.id.adminParty,
                Seq(reassignedCid),
                daId,
                acmeId,
              )

          def reassignmentStore(
              participant: LocalParticipantReference,
              synchronizerId: SynchronizerId,
          ): ReassignmentStore =
            participant.underlying.value.sync.syncPersistentStateManager
              .get(synchronizerId)
              .value
              .reassignmentStore

          // Retrieve the reassignment data
          val reassignmentStoreP1Acme = reassignmentStore(participant1, acmeId)
          val incompleteUnassignment = eventually() {
            reassignmentStoreP1Acme
              .lookup(unassignedWrapper.reassignmentId)
              .value
              .futureValueUS
              .value
          }

          participant1.ledger_api.commands.submit_assign(
            participant1.id.adminParty,
            unassignedWrapper.unassignId,
            daId,
            acmeId,
          )

          logger.info("Check that reassignment has completed")
          assertInAcsSync(Seq(participant1), acmeName, reassignedCid)

          // We do the following pings to ensure that the clean request timestamp on the two synchronizers is past the "common time"
          // when the assignment happens on the destination synchronizer.
          participant1.health.ping(participant2, synchronizerId = Some(daId))
          participant1.health.ping(participant2, synchronizerId = Some(acmeId))

          logger.info("Create three contracts on synchronizer acme")
          val c1 = deployAndCheckContract(acmeId)
          val c2 = deployAndCheckContract(acmeId)
          val c3 = deployAndCheckContract(acmeId)
          val createdCidsAcme = Seq(c1, c2, c3)

          logger.info(
            s"Inspect the states of the contracts on acme and the reassigned contract w.r.t. timestamp before" +
              s"the reassign $tsBeforeReassign and synchronizer da"
          )
          val queriedContracts = createdCidsAcme.map(_.id.toLf) ++ Seq(reassignedCid)
          val inspectContracts = participant1.commitments.inspect_commitment_contracts(
            queriedContracts,
            tsBeforeReassign,
            daId,
            downloadPayload = true,
          )

          logger.info(
            s"The result contains one entry per acme contracts and two entries for the reassigned contract (one per synchronizer)"
          )
          inspectContracts.size shouldBe createdCidsAcme.size + Seq(reassignedCid).size * 2

          logger.info(
            s"All acme contracts have one state (created), have a payload, and do not exist on da"
          )
          inspectContracts.filter(s =>
            !s.activeOnExpectedSynchronizer &&
              s.contract.isDefined &&
              s.state.sizeIs == 1 &&
              s.state.count(cs =>
                cs.contractState.isInstanceOf[ContractCreated] && cs.synchronizerId == acmeId
              ) == 1
          ) should have size (createdCidsAcme.size).toLong

          logger.info(
            s"The reassigned contract has two states (created and unassigned) on da, has a payload, and is" +
              s"active on da at the queried time"
          )
          val reassigned = inspectContracts.filter(c => c.cid == reassignedCid)
          val reassignedCounter =
            incompleteUnassignment.contracts.contractIdCounters.toMap.apply(reassignedCid)
          reassigned.size shouldBe 2
          reassigned.filter(states =>
            states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractCreated] && s.synchronizerId == daId
              ) == 1 &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractUnassigned] && s.synchronizerId == daId &&
                  s.contractState
                    .asInstanceOf[ContractUnassigned]
                    .reassignmentId
                    .contains(incompleteUnassignment.reassignmentId) &&
                  s.contractState
                    .asInstanceOf[ContractUnassigned]
                    .reassignmentCounterSrc == reassignedCounter - 1
              ) == 1 &&
              states.state.sizeIs == 2
          ) should have size 1

          logger.info(
            s"The reassigned contract has one state (assigned) on acme, has a payload, and is" +
              s"active on da at the queried time"
          )
          reassigned.filter(states =>
            states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractAssigned] && s.synchronizerId == acmeId &&
                  s.contractState
                    .asInstanceOf[ContractAssigned]
                    .reassignmentId
                    .contains(incompleteUnassignment.reassignmentId) &&
                  s.contractState
                    .asInstanceOf[ContractAssigned]
                    .reassignmentCounterTarget == reassignedCounter
              ) == 1 &&
              states.state.sizeIs == 1
          ) should have size 1
          reassigned.filter(c => c.contract.isDefined) should have size 2

          logger.info(
            s"Inspect the states of the reassigned contract w.r.t. timestamp after unassign and before assign" +
              s"${incompleteUnassignment.unassignmentTs} and synchronizer da"
          )
          val inspectContracts2 = participant1.commitments.inspect_commitment_contracts(
            Seq(reassignedCid),
            incompleteUnassignment.unassignmentTs,
            daId,
            downloadPayload = true,
          )

          logger.info(
            s"The reassigned contract has two states (created and unassigned) on da, has a payload, and is" +
              s"not active on da at the queried time"
          )
          val reassigned2 = inspectContracts2.filter(c => c.cid == reassignedCid)

          reassigned2.size shouldBe 2
          reassigned2.filter(states =>
            !states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractCreated] && s.synchronizerId == daId
              ) == 1 &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractUnassigned] && s.synchronizerId == daId
              ) == 1 &&
              states.state.sizeIs == 2
          ) should have size 1

          logger.info(
            s"The reassigned contract has one state (assigned) on acme, has a payload, and is" +
              s"not active on da at the queried time"
          )
          reassigned2.filter(states =>
            !states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractAssigned] && s.synchronizerId == acmeId
              ) == 1 &&
              states.state.sizeIs == 1
          ) should have size 1
          reassigned2.filter(c => c.contract.isDefined) should have size 2
      }
    }
  }

  "Removed participant should not trigger commitment computations" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value

    val cmd =
      new Iou(
        participant1.adminParty.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
        new Amount(3.50.toBigDecimal, "CHF"),
        List().asJava,
      ).create.commands.asScala.toSeq
    participant1.ledger_api.javaapi.commands.submit(
      Seq(participant1.id.adminParty),
      cmd,
      Some(daId),
    )
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        logger.info("Removing participant1 on synchronizer da")
        participant1.topology.synchronizer_trust_certificates.propose(
          participantId = participant1.id,
          synchronizerId = initializedSynchronizers(daName).synchronizerId,
          change = TopologyChangeOp.Remove,
        )
        eventually() {
          participant1.synchronizers.is_connected(
            initializedSynchronizers(daName).synchronizerId
          ) shouldBe false
        }
        logger.info("Successfully disconnected participant1 from synchronizer da")

        // Sit out for a full commitment period, so that we can later check that the participants don't exchange
        // messages for this period
        val tickNoCommitments1 = tickAfter(simClock.uniqueTime())
        val tickNoCommitments2 = tickAfter(tickNoCommitments1.forgetRefinement)
        simClock.advanceTo(tickNoCommitments2.forgetRefinement.immediateSuccessor)

        simClock.advance(interval.plus(JDuration.ofSeconds(1)))
        participants.local.foreach(_.testing.fetch_synchronizer_times())

        val startNoCommitment = tickNoCommitments1.toInstant
        participant1.commitments
          .computed(
            daName,
            startNoCommitment,
            Instant.now(),
            Some(participant1),
          ) shouldBe empty
        participant1.commitments
          .received(
            daName,
            startNoCommitment,
            Instant.now(),
            Some(participant2),
          ) shouldBe empty
        participant2.commitments
          .received(
            daName,
            startNoCommitment,
            Instant.now(),
            Some(participant1),
          ) shouldBe empty
        participant2.commitments
          .computed(
            daName,
            startNoCommitment,
            Instant.now(),
            Some(participant1),
          ) shouldBe empty
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
          ))
      },
    )
  }
}

class AcsCommitmentProcessorReferenceIntegrationTestPostgres
    extends AcsCommitmentProcessorIntegrationTest {
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

class AcsCommitmentProcessorReferenceIntegrationTestH2
    extends AcsCommitmentProcessorIntegrationTest {
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
