// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import cats.syntax.traverse.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.performance.{PerformanceRunner, RateSettings}
import org.scalatest.time.{Millis, Minutes, Span}

import scala.concurrent.duration.DurationInt

/** Ensures performance runner can introduce reassignments
  */
class MultiSynchronizerPerformanceRunnerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  import BasePerformanceIntegrationTest.*

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  // Shorten the interval, as the default is 1 minute
  private val sequencerClientAcknowledgementIntervalMs: Int = 5000

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateTargetTimestampForwardTolerance(30.seconds),
        ConfigTransforms.updateSequencerClientAcknowledgementInterval(
          config.NonNegativeDuration
            .ofMillis(sequencerClientAcknowledgementIntervalMs.toLong)
            .toInternal
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.synchronizers.connect_local(sequencer2, acmeName)
        participant1.health.ping(participant2, synchronizerId = Some(daId))
        participant1.health.ping(participant2, synchronizerId = Some(acmeId))
      }

  // Be extra patient for the completion of the performance runner future
  override implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Minutes)), interval = scaled(Span(500, Millis)))

  "performance runner should trigger reassignments" in { implicit env =>
    import env.*

    val ledgerEndInitial = participant1.ledger_api.state.end()

    val (p1Config, p2Config) =
      defaultConfigs(
        0,
        participant1,
        participant2,
        totalCycles = 40,
        otherSynchronizers = Seq(acmeId),
        otherSynchronizersRatio = 0.9,
        rateSettings = RateSettings.defaults,
      )

    val runnerP1 =
      new PerformanceRunner(
        p1Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1"),
      )
    val runnerP2 =
      new PerformanceRunner(
        p2Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2"),
      )

    val runners = Seq(runnerP1, runnerP2)
    runners.foreach(env.environment.addUserCloseable(_))

    loggerFactory.assertLogsUnorderedOptional(
      runners.map(_.startup()).sequence.map(_.sequence).map(_.map(_ => ())).futureValue,
      // The test generates a lot of contention on the 'insert block' DB operation
      LogEntryOptionality.Optional -> (_.warningMessage should include(
        "The operation 'insert block' has failed with an exception"
      )),
      LogEntryOptionality.Optional -> (_.warningMessage should include(
        "Now retrying operation 'insert block'"
      )),
    )

    val trader = participant1.topology.party_to_participant_mappings
      .list(daId)
      .find(_.item.partyId.toString.contains("Tradie"))
      .map(_.item.partyId)
      .value

    val traderUnassignments = participant1.ledger_api.updates
      .reassignments(
        partyIds = Set(trader),
        completeAfter = 100,
        beginOffsetExclusive = ledgerEndInitial,
      )
      .filter(_.isUnassignment)

    traderUnassignments should not be empty

    runners.foreach(_.close())
  }
}
