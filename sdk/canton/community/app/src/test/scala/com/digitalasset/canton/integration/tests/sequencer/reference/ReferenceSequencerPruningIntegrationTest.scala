// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseConfigTransforms,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.pruning.SequencerPruningIntegrationTest

class ReferenceSequencerPruningIntegrationTest extends SequencerPruningIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(
    new UseConfigTransforms(
      Seq(
        reduceSequencerClientAcknowledgementInterval,
        increaseParticipant3AcknowledgementInterval,
        reduceSequencerAcknowledgementConflateWindow,
        ConfigTransforms.useStaticTime,
      ),
      loggerFactory,
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        // Enable traffic control so that we can assert that traffic data is also pruned
        val trafficControlParameters = TrafficControlParameters(
          maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(200 * 1000L),
          readVsWriteScalingFactor = PositiveInt.one,
          maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
          setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
          enforceRateLimiting = true,
          baseEventCost = NonNegativeLong.zero,
        )
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(trafficControl = Some(trafficControlParameters)),
        )
        sequencer1.topology.synchronisation.await_idle()

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)
      }

  override protected val pruningRegex: String =
    """Removed ([1-9]\d*) blocks
        |Removed at least ([1-9]\d*) events, at least (\d+) payloads, at least ([1-9]\d*) counter checkpoints
        |Removed ([0-9]\d*) traffic purchased entries
        |Removed ([1-9]\d*) traffic consumed entries""".stripMargin

  override protected val pruningNothing: String =
    """Removed 0 blocks
        |Removed at least 0 events, at least 0 payloads, at least 0 counter checkpoints
        |Removed 0 traffic purchased entries
        |Removed 0 traffic consumed entries""".stripMargin

  override protected val pruningRegexWithTrafficPurchase =
    """Removed ([1-9]\d*) blocks
        |Removed at least ([1-9]\d*) events, at least (\d+) payloads, at least ([1-9]\d*) counter checkpoints
        |Removed ([1-9]\d*) traffic purchased entries
        |Removed ([1-9]\d*) traffic consumed entries""".stripMargin
}
