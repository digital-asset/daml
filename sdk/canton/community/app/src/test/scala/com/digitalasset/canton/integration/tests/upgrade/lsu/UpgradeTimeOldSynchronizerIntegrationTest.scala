// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.functor.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.SingleSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SequencerReader}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.Future

/*
 * This test validates that upgrade time is respected on the old synchronizer.
 * More precisely, after the upgrade time:
 *
 * - sequencers: only emit time proofs
 * - participants: time is not signalled to the indexer
 */
class UpgradeTimeOldSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = SingleSynchronizer,
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private val upgradeTime = CantonTimestamp.Epoch.plusSeconds(60)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.automaticallyPerformLogicalSynchronizerUpgrade).replace(false)
        )
      )

  "Upgrade time on old synchronizer" should {
    "is respected by sequencers and participants" in { implicit env =>
      import env.*
      val successorPSId = daId.copy(serial = NonNegativeInt.one)

      participant1.synchronizers.connect_local(sequencer1, daName)

      synchronizerOwners1.foreach(
        _.topology.synchronizer_upgrade.announcement.propose(successorPSId, upgradeTime)
      )

      eventually() {
        participant1.topology.synchronizer_upgrade.announcement
          .list(daId)
          .loneElement
          .item
          .upgradeTime shouldBe upgradeTime
      }

      mediator1.stop()
      sequencer1.stop()
      loggerFactory.assertLogsSeq(
        SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[SequencerReader]
      )
      (
        {
          sequencer1.start()
          sequencer1.health.wait_for_running()
          mediator1.start()
          mediator1.health.wait_for_running()
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.infoMessage should include(
                "Updating synchronizer upgrade information, setting new successor from None to Some"
              ),
              "reinitialize synchronizer upgrade information after sequencer restart",
            )
          )
        ),
      )

      /** If the participant sends the ping before the resilient subscription detects that the
        * sequencer is up again, it fails (failed future). On such failed future, the submission is
        * not removed from the pending submissions.
        *
        * Then, the retry loop of the ping service kicks in for the resubmission. However, since the
        * command is already in-flight, it is not re-sent to the sequencer. We retry over and over
        * again.
        *
        * In wall clock, a timeout would occur somewhere (observing a sequenced time > max
        * sequencing time or timeout of the console command and everything would be fine).
        */
      participant1.synchronizers.disconnect_all()
      participant1.synchronizers.reconnect_all()

      participant1.health.ping(participant1)

      environment.simClock.value.advanceTo(upgradeTime)

      var pingF: Future[Option[Unit]] = Future.successful(None)

      loggerFactory.assertEventuallyLogsSeq(
        (SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[SequencerReader])
          || (SuppressionRule.Level(Level.WARN) && SuppressionRule.forLogger[TransactionProcessor])
      )(
        {
          // asynchronously launch a ping. unfortunately, the action passed to assertEventuallyLogsSeq must be a type other than
          // Future[*], otherwise BaseTest.eventually does some magic that causes the consecutive calls to assertEventuallyLogsSeq to "overlap"
          logger.debug("Starting asynchronous ping")
          pingF = Future(
            participant1.health
              .maybe_ping(participant1, timeout = config.NonNegativeDuration.ofSeconds(1))
              .void
          )
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.infoMessage should include(
                s"Computed synchronizer upgrade time offset: 1m"
              ),
              "upgrade time offset computation",
            ),
            (
              _.infoMessage should include(
                "Delivering an empty event instead of the original, because it was sequenced at or after the upgrade time."
              ),
              "event after upgrade time warning",
            ),
            (
              _.warningMessage should include(
                "Submission timed out at 1970-01-01T00:02"
              ),
              "immediate timeout due to upgrade time offset",
            ),
          )
        ),
      )

      pingF.futureValue shouldBe None
      logger.debug("Ping failed")
      participant1.testing.fetch_synchronizer_times()

      val dynamicSynchronizerParameters = participant1.topology.synchronizer_parameters.latest(daId)

      sequencer1.underlying.value.sequencer.timeTracker
        .fetchTime()
        .futureValueUS should be >= upgradeTime.plus(
        dynamicSynchronizerParameters.decisionTimeout.asJava
      )

      val cleanSynchronizerIndex = participant1.underlying.value.sync.stateInspection
        .getAcsInspection(daId)
        .value
        .ledgerApiStore
        .value
        .cleanSynchronizerIndex(daId)
        .futureValueUS
        .value

      /** Despite the call to fetch time and the empty message triggered by the ping (ping messages
        * are replaced by empty messages on the read side of the sequencer), time should not
        * progress past upgrade time from indexer point of view. Instead, last events are replaced
        * by
        * [[com.digitalasset.canton.ledger.participant.state.Update.LogicalSynchronizerUpgradeTimeReached]]
        * with record time equal upgrade time.
        */
      cleanSynchronizerIndex.recordTime shouldBe upgradeTime
      cleanSynchronizerIndex.sequencerIndex.value.sequencerTimestamp should be < upgradeTime
    }
  }
}
