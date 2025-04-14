// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import monocle.macros.syntax.lens.*

sealed trait MultipleProtocolVersionReassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  // Test topology transactions against the latest two stable versions (we don't exclude that the two are equal)
  // TODO(#16458) Change to the commented code below when we have two stable protocol versions
  private lazy val (beforeLastStable, lastStable) = (ProtocolVersion.v33, ProtocolVersion.v33)
//  private lazy val (beforeLastStable, lastStable) = {
//    val lastTwoStables = ProtocolVersion.stableAndSupported.sorted.takeRight(2)
//    if (lastTwoStables.sizeIs == 2)
//      (lastTwoStables(0), lastTwoStables(1))
//    else (lastTwoStables(0), lastTwoStables(0))
//  }

  private lazy val alphaProtocolVersion: ProtocolVersion = ProtocolVersion.alpha.min1
  private lazy val devProtocolVersion: ProtocolVersion = ProtocolVersion.dev

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") {
          _.focus(_.parameters.minimumProtocolVersion)
            .replace(Some(ParticipantProtocolVersion(beforeLastStable)))
        }
      )
      .addConfigTransforms(ConfigTransforms.dontWarnOnDeprecatedPV*)
      .withSetup { implicit env =>
        import env.*

        participant1.dars.upload(CantonExamplesPath)
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.synchronizers.connect_local(sequencer3, alias = repairSynchronizerName)
        participant1.synchronizers.connect_local(sequencer4, alias = devSynchronizerName)
      }

  private def createCycleCommand(
      id: String
  )(implicit env: TestConsoleEnvironment): Command = {
    import env.*
    val pkg = participant1.packages.find_by_module("Cycle").headOption.value
    ledger_api_utils.create(
      pkg.packageId,
      "Cycle",
      "Cycle",
      Map[String, Any]("owner" -> participant1.adminParty, "id" -> id),
    )
  }

  // Reassign the Cycle contract from `source` to `target` synchronizer
  private def reassign(
      sourceId: SynchronizerId,
      targetId: SynchronizerId,
      sourceAlias: SynchronizerAlias,
      targetAlias: SynchronizerAlias,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val party = participant1.adminParty

    // Cycle contract creation
    participant1.ledger_api.commands.submit(
      Seq(party),
      Seq(createCycleCommand("cycle-contract1")),
      Some(sourceId),
    )

    val cid1 = searchAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = sourceAlias,
      module = "Cycle",
      template = "Cycle",
    )

    def executeReassignment(): Unit =
      participant1.ledger_api.commands.submit_reassign(party, Seq(cid1), sourceId, targetId)

    executeReassignment()

    searchAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = targetAlias,
      module = "Cycle",
      template = "Cycle",
    )

    // Cycle contract archival
    val cid2 = participant1.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(party)
    participant1.ledger_api.javaapi.commands.submit(
      Seq(party),
      Seq(cid2.id.exerciseArchive().commands.loneElement),
      Some(targetId),
    )

    assertNotInAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = targetAlias,
      module = "Cycle",
      template = "Cycle",
    )
  }

  "A contract created on a synchronizer" should {
    "be reassignable to synchronizers running on different protocol versions" in { implicit env =>
      import env.*

      val synchronizerForPV: Map[ProtocolVersion, (SynchronizerId, SynchronizerAlias)] = Map(
        beforeLastStable -> (daId, daName),
        lastStable -> (acmeId, acmeName),
        alphaProtocolVersion -> (repairSynchronizerId, repairSynchronizerName),
        devProtocolVersion -> (devSynchronizerId, devSynchronizerName),
      )

      val protocolVersions = synchronizerForPV.keysIterator.toList

      // 2-permutations (`permutations` doesn't support it)
      val possibleReassignments =
        for {
          source <- protocolVersions
          target <- protocolVersions
          if source != target
        } yield (source, target)

      forAll(possibleReassignments) { case (sourcePV, targetPV) =>
        clue(s"Reassigning from $sourcePV to $targetPV") {
          reassign(
            synchronizerForPV(sourcePV)._1,
            synchronizerForPV(targetPV)._1,
            synchronizerForPV(sourcePV)._2,
            synchronizerForPV(targetPV)._2,
          )
        }
      }
    }
  }
}

class MultipleProtocolVersionReassignmentIntegrationTestPostgres
    extends MultipleProtocolVersionReassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(
        Set("sequencer1"),
        Set("sequencer2"),
        Set("sequencer3"),
        Set("sequencer4"),
      ),
    )
  )
}

//class MultipleProtocolVersionReassignmentIntegrationTestH2
//    extends MultipleProtocolVersionReassignmentIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3"), Set("sequencer4")),
//    )
//  )
//}
