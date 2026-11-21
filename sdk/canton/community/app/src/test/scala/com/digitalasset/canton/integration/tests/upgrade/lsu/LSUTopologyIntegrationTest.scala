// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.TopologyMapping

/*
 * This test is used to test topology related aspects of LSU.
 */
abstract class LSUTopologyIntegrationTest extends LSUBase {

  override protected def testName: String = "logical-synchronizer-upgrade"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "logical upgrade state cannot be queried if no upgrade is ongoing" in { implicit env =>
      assertThrowsAndLogsCommandFailures(
        env.sequencer1.topology.transactions.logical_upgrade_state(),
        _.shouldBeCommandFailure(
          TopologyManagerError.NoOngoingSynchronizerUpgrade,
          "The operation cannot be performed because no upgrade is ongoing",
        ),
      )
    }

    "the upgrade time must be sufficiently in the future" in { implicit env =>
      import env.*

      synchronizerOwners1.foreach(owner =>
        assertThrowsAndLogsCommandFailures(
          owner.topology.synchronizer_upgrade.announcement
            .propose(
              daId.copy(serial = NonNegativeInt.one),
              CantonTimestamp.Epoch.minusSeconds(10),
            ),
          _.shouldBeCommandFailure(TopologyManagerError.InvalidUpgradeTime),
        )
      )
    }

    "work end-to-end" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()
      val newPSId = fixture.newPSId
      val newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters

      synchronizerOwners1.foreach(
        _.topology.synchronizer_upgrade.announcement.propose(newPSId, upgradeTime)
      )

      migrateSynchronizerNodes(fixture)

      sequencer2.synchronizer_parameters.static.get() shouldBe newStaticSynchronizerParameters
      val allProposals = sequencer2.topology.transactions.list(
        newPSId,
        proposals = true,
        timeQuery = TimeQuery.Range(None, None),
      )
      forAll(allProposals.result)(proposal => proposal.validUntil shouldBe empty)

      val allLSUMappings = sequencer2.topology.transactions.list(
        newPSId,
        filterMappings = TopologyMapping.Code.logicalSynchronizerUpgradeMappings.toSeq,
        timeQuery = TimeQuery.Range(None, None),
      )
      allLSUMappings.result shouldBe empty
    }
  }
}

final class LSUTopologyReferenceIntegrationTest extends LSUTopologyIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}

final class LSUTopologyBftOrderingIntegrationTest extends LSUTopologyIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
