// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.IntegrationTestUtilities.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}

sealed trait MultiSynchronizerPingIntegrationTests
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasCycleUtils {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1

  private val expectedPingTransactionCount = 2

  "select a synchronizer for ping" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)

    val p1InitialCounts = grabCounts(daName, participant1)
    val p2InitialCounts = grabCounts(daName, participant2)

    assertPingSucceeds(participant1, participant2, synchronizerId = Some(daId))

    eventually() {
      val p1Counts = grabCounts(daName, participant1)
      val p2Counts = grabCounts(daName, participant2)
      assertResult(expectedPingTransactionCount)(
        p1Counts.acceptedTransactionCount - p1InitialCounts.acceptedTransactionCount
      )
      assertResult(p1Counts.plus(p2InitialCounts))(p2Counts.plus(p1InitialCounts))
    }

    assertPingSucceeds(participant1, participant2, synchronizerId = Some(acmeId))

    eventually() {
      val p1Counts = grabCounts(acmeName, participant1)
      val p2Counts = grabCounts(acmeName, participant2)
      assertResult(expectedPingTransactionCount)(
        p1Counts.acceptedTransactionCount - p1InitialCounts.acceptedTransactionCount
      )
      assertResult(p1Counts.plus(p2InitialCounts))(p2Counts.plus(p1InitialCounts))
    }
  }

}

//class MultiSynchronizerPingIntegrationTestsDefault extends MultiSynchronizerPingIntegrationTests

class MultiSynchronizerPingReferenceIntegrationTestsPostgres
    extends MultiSynchronizerPingIntegrationTests {
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

class MultiSynchronizerPingBftOrderingIntegrationTestsPostgres
    extends MultiSynchronizerPingIntegrationTests {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
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
