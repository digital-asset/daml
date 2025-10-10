// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

/** This trait provides helpers for the logical synchronizer upgrade tests. The main goal is to
  * improve readability of each tests by focusing on the behavior we want to test and make it easier
  * to write new tests.
  */
trait LSUBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with LogicalUpgradeUtils {

  registerPlugin(new UsePostgres(loggerFactory))

  protected var oldSynchronizerNodes: SynchronizerNodes = _
  protected var newSynchronizerNodes: SynchronizerNodes = _
  protected def newOldSequencers: Map[String, String]
  protected def newOldMediators: Map[String, String]
  protected def newOldNodesResolution: Map[String, String] =
    newOldSequencers ++ newOldMediators

  protected def upgradeTime: CantonTimestamp

  protected def configTransforms: List[ConfigTransform] = newOldSequencers.keySet
    .map(sequencerName =>
      ConfigTransforms
        .updateSequencerConfig(sequencerName)(
          _.focus(_.parameters.sequencingTimeLowerBoundExclusive).replace(Some(upgradeTime))
        )
    )
    .toList
    ++ List(
      ConfigTransforms.disableAutoInit(newOldNodesResolution.keySet),
      ConfigTransforms.useStaticTime,
    )

  protected def fixtureWithDefaults(upgradeTime: CantonTimestamp = upgradeTime)(implicit
      env: TestConsoleEnvironment
  ): Fixture = {
    val currentPSId = env.daId

    Fixture(
      currentPSId = currentPSId,
      upgradeTime = upgradeTime,
      oldSynchronizerNodes = oldSynchronizerNodes,
      newSynchronizerNodes = newSynchronizerNodes,
      newOldNodesResolution = newOldNodesResolution,
      oldSynchronizerOwners = env.synchronizerOwners1,
      newPV = ProtocolVersion.dev,
      // increasing the serial as well, so that the test also works when running with PV=dev
      newSerial = currentPSId.serial.increment.toNonNegative,
    )
  }

  /** Perform synchronizer side of the LSU:
    *
    *   - Upgrade announcement
    *   - Migration of synchronizer nodes
    *   - Sequencer successors announcements
    */
  protected def performSynchronizerNodesLSU(
      fixture: Fixture
  ): Unit = {
    fixture.oldSynchronizerOwners.foreach(
      _.topology.synchronizer_upgrade.announcement.propose(fixture.newPSId, fixture.upgradeTime)
    )

    migrateSynchronizerNodes(fixture)

    fixture.oldSynchronizerNodes.sequencers.zip(fixture.newSynchronizerNodes.sequencers).foreach {
      case (oldSequencer, newSequencer) =>
        oldSequencer.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
          sequencerId = oldSequencer.id,
          endpoints = newSequencer.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          synchronizerId = fixture.currentPSId,
        )
    }
  }

  /** Instantiate the new synchronizer nodes with identity of the previous ones and import topology
    * state.
    */
  protected def migrateSynchronizerNodes(
      fixture: Fixture
  ): Unit = {
    exportNodesData(
      SynchronizerNodes(
        sequencers = fixture.oldSynchronizerNodes.sequencers,
        mediators = fixture.oldSynchronizerNodes.mediators,
      )
    )

    // Migrate nodes preserving their data (and IDs)
    fixture.newSynchronizerNodes.all.foreach { newNode =>
      migrateNode(
        migratedNode = newNode,
        newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
        synchronizerId = fixture.currentPSId,
        newSequencers = fixture.newSynchronizerNodes.sequencers,
        exportDirectory = baseExportDirectory,
        sourceNodeNames = fixture.newOldNodesResolution,
      )
    }
  }
}

private[lsu] object LSUBase {
  final case class Fixture(
      currentPSId: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      oldSynchronizerNodes: SynchronizerNodes,
      newSynchronizerNodes: SynchronizerNodes,
      newOldNodesResolution: Map[String, String],
      oldSynchronizerOwners: Set[InstanceReference],
      newPV: ProtocolVersion,
      newSerial: NonNegativeInt,
  ) {
    val newStaticSynchronizerParameters: StaticSynchronizerParameters =
      StaticSynchronizerParameters.defaultsWithoutKMS(
        newPV,
        newSerial,
        topologyChangeDelay = config.NonNegativeFiniteDuration.Zero,
      )

    val newPSId: PhysicalSynchronizerId =
      PhysicalSynchronizerId(currentPSId.logical, newStaticSynchronizerParameters.toInternal)

    val synchronizerSuccessor: SynchronizerSuccessor = SynchronizerSuccessor(newPSId, upgradeTime)
  }
}
