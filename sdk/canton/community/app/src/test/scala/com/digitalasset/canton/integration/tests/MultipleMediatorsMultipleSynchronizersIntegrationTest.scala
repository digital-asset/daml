// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import org.scalactic.source.Position

final class MultipleMediatorsMultipleSynchronizersIntegrationTest
    extends CommunityIntegrationTest
    with HasCycleUtils
    with SharedEnvironment
    with HasProgrammableSequencer
    with MultipleMediatorsBaseTest
    with AcsInspection {

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(
      numParticipants = 1,
      numSequencers = 2,
      numMediators = 4,
    ).withManualStart
      .addConfigTransform(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
      )
      .withSetup { implicit env =>
        import env.*

        participant1.start()
        sequencers.local.start()
        mediators.local.start()

        val synchronizer1 = daName
        val synchronizer2 = acmeName

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName.unwrap,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )(env).bootstrap()

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            acmeName.unwrap,
            synchronizerOwners = Seq(sequencer2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer2),
            mediators = Seq(mediator3),
          )
        )(env).bootstrap()

        participant1.synchronizers.connect_local(sequencer1, alias = synchronizer1)
        participant1.synchronizers.connect_local(sequencer2, alias = synchronizer2)
        participant1.dars.upload(CantonExamplesPath)

        participantSeesMediators(participant1, Set(Set(mediator1.id), Set(mediator3.id)))

        val synchronizer1Id = daId
        val synchronizer2Id = acmeId

        participant1.health.ping(participant1, synchronizerId = Some(synchronizer1Id))
        participant1.health.ping(participant1, synchronizerId = Some(synchronizer2Id))

        val activeMediators = Seq(mediator1, mediator3)
        val activeMediatorIds = activeMediators.map(_.id)
        activeMediatorIds.distinct shouldBe activeMediatorIds
      }

  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  // TODO(#15875): add checks that completions are emitted
  "Two synchronizers with two mediators each" when {
    "the mediator is switched out during reassignment submission" in { implicit env =>
      import env.*

      val cycle =
        new Cycle(
          "reassign-contract",
          participant1.adminParty.toProtoPrimitive,
        ).create.commands.loneElement
      val tx = participant1.ledger_api.javaapi.commands.submit_flat(
        Seq(participant1.adminParty),
        Seq(cycle),
        synchronizerId = Some(synchronizer1Id),
      )
      val Seq(contract) = (JavaDecodeUtil.decodeAllCreated(Cycle.COMPANION)(tx): @unchecked)

      logger.info("Switch out the mediator during unassignment")

      val unassignF = switchMediatorDuringSubmission(
        sequencer1,
        NonNegativeInt.zero,
        NonNegativeInt.one,
        mediator2,
        participant1,
      ) { () =>
        participant1.testing.fetch_synchronizer_time(synchronizer2Id)

        a[CommandFailure] shouldBe thrownBy {
          participant1.ledger_api.commands.submit_unassign(
            participant1.adminParty,
            contract.id.toLf,
            synchronizer1Id,
            synchronizer2Id,
          )
        }
      }

      val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(4))
      unassignF.futureValue(patience, implicitly[Position])

      logger.debug("Switch out the mediator during assignment")

      val assignF = switchMediatorDuringSubmission(
        sequencer2,
        NonNegativeInt.zero,
        NonNegativeInt.one,
        mediator4,
        participant1,
      ) { () =>
        val unassigned = participant1.ledger_api.commands.submit_unassign(
          participant1.adminParty,
          contract.id.toLf,
          synchronizer1Id,
          synchronizer2Id,
        )

        a[CommandFailure] shouldBe thrownBy {
          participant1.ledger_api.commands.submit_assign(
            participant1.adminParty,
            unassigned.unassignedEvent.unassignId,
            synchronizer1Id,
            synchronizer2Id,
          )
        }
      }

      assignF.futureValue(patience, implicitly[Position])

      logger.info("Look for the aborted reassignment and complete it")
      val Seq(unassignedEvent) = (participant1.ledger_api.state.acs
        .incomplete_unassigned_of_party(participant1.adminParty): @unchecked)

      unassignedEvent.contractId shouldBe contract.id.toLf.coid

      participant1.ledger_api.commands.submit_assign(
        participant1.adminParty,
        unassignedEvent.unassignId,
        synchronizer1Id,
        synchronizer2Id,
      )
    }
  }
}
