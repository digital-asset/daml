// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.java.automaticreassignmenttransactions as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.SynchronizerRouterIntegrationTestSetup
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  PartiesAllocator,
  PartyToParticipantDeclarative,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*

/*
  The goal is to check that decentralized parties can submit/trigger
  explicit as well as automatic reassignments.
 */
class AutomaticReassignmentDecentralizedPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  private var decentralizedParty: PartyId = _
  private var aggregate: M.Aggregate.ContractId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1.withSetup { implicit env =>
      import env.*

      def disableAssignmentExclusivityTimeout(d: InitializedSynchronizer): Unit =
        d.synchronizerOwners.foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              d.synchronizerId,
              _.update(
                assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
                topologyChangeDelay = config.NonNegativeFiniteDuration.Zero,
              ),
            )
        )

      disableAssignmentExclusivityTimeout(getInitializedSynchronizer(daName))
      disableAssignmentExclusivityTimeout(getInitializedSynchronizer(acmeName))

      participants.all.dars.upload(CantonTestsPath)
      participants.all.synchronizers.connect_local(sequencer1, daName)
      participants.all.synchronizers.connect_local(sequencer2, acmeName)

      participant1.health.ping(participant2, synchronizerId = Some(daId))
      participant1.health.ping(participant2, synchronizerId = Some(acmeId))

      val dso = "dso"
      // multi host dso on two participant on the two synchronizers
      PartiesAllocator(participants.all.toSet)(
        Seq(dso -> participant1),
        Map(
          dso -> Map(
            daId -> (PositiveInt.one, Set(
              (participant1.id, ParticipantPermission.Submission),
              (participant2.id, ParticipantPermission.Submission),
            )),
            acmeId -> (PositiveInt.one, Set(
              (participant1.id, ParticipantPermission.Submission),
              (participant2.id, ParticipantPermission.Submission),
            )),
          )
        ),
      )
      decentralizedParty = dso.toPartyId(participant1)
    }

  /** Ensure contract with id `cid` is assigned to synchronizer `synchronizerId`
    */
  private def ensureContractIsAssignedTo(cid: String, synchronizerId: SynchronizerId)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    val contracts =
      env.participant1.ledger_api.state.acs.of_party(decentralizedParty).filter(_.contractId == cid)
    contracts.loneElement.synchronizerId.value shouldBe synchronizerId
  }

  "Automatic reassignment to acme" in { implicit env =>
    import env.*

    val singles = (0 until 2).map(_ =>
      SynchronizerRouterIntegrationTestSetup
        .createSingle(
          owner = decentralizedParty,
          synchronizerId = Some(daId),
          participant = participant1,
          coordinator = decentralizedParty,
        )
        .id
    )

    SynchronizerRouterIntegrationTestSetup
      .createSingle(
        owner = decentralizedParty,
        synchronizerId = Some(daId),
        participant = participant1,
        coordinator = decentralizedParty,
      )
      .id

    aggregate = SynchronizerRouterIntegrationTestSetup
      .createAggregate(
        participant1,
        decentralizedParty,
        singles,
        synchronizerId = Some(daId),
        additionalParty = Some(participant1.adminParty),
      )
      .id

    // increase the threshold to 2
    Seq(daId, acmeId).foreach { synchronizerId =>
      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), synchronizerId)(
        participant1,
        decentralizedParty,
        PositiveInt.two,
        Set(
          (participant1.id, ParticipantPermission.Submission),
          (participant2.id, ParticipantPermission.Submission),
        ),
      )
    }

    ensureContractIsAssignedTo(aggregate.contractId, daId)

    val exerciseCmd = aggregate.exerciseCountAllAdditionalParty().commands().asScala.toSeq

    participant1.ledger_api.javaapi.commands.submit(
      actAs = Seq(participant1.adminParty),
      readAs = Seq(decentralizedParty),
      commands = exerciseCmd,
      synchronizerId = Some(acmeId), // we want automatic reassignment
    )
  }

}
