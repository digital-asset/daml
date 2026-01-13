// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{ForceFlag, PartyId}

import scala.jdk.CollectionConverters.*

/** The goal of this test is to reproduce the conditions that lead to:
  * https://github.com/DACH-NY/canton/pull/29052
  *
  * Topology client was wrongly initialized and accessed under the following conditions:
  *   - Mediator node onboarding at t1_seq, t1_eff
  *   - Concurrent topology changes (have to be proposals) at t2_seq, t2_eff, so that t1_seq <
  *     t2_seq < t1_eff
  *   - Concurrent confirmation traffic reaching the new mediator at t3, with t1_eff < t3 < t2_eff
  *
  * Wrongly initialized topology client accesses at t3 an interval with expected upper limit at
  * t2_eff, but since it is a proposal, the latest topology change is at t1_eff, so upper bound is
  * not found, which leads to an `IllegalStateException`, mentioning "expected to have an upper
  * bound".
  */
trait MediatorOnboardingConcurrentTrafficTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 1,
        numMediators = 2,
      )
      .addConfigTransform(ConfigTransforms.disableOnboardingTopologyValidation)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        clue("participants connect to sequencer1") {
          participants.local.synchronizers.connect_local(sequencer1, daName)
        }
      }

  "Onboard the mediator with a concurrent topology txs and confirmations" in { implicit env =>
    import env.*
    participant1.dars.upload(CantonExamplesPath)
    participant2.dars.upload(CantonExamplesPath)
    participant3.dars.upload(CantonExamplesPath)

    val AliceName = "Alice"

    PartiesAllocator(Set(participant1, participant2, participant3))(
      newParties = Seq(AliceName -> participant1.id),
      targetTopology = Map(
        AliceName -> Map(
          synchronizer1Id -> (PositiveInt.tryCreate(1) -> Set(
            participant1.id -> ParticipantPermission.Submission,
            participant2.id -> ParticipantPermission.Observation,
          ))
        )
      ),
    )
    val alice = AliceName.toPartyId(participant1)

    // Not sure why, but without this confirmation, the issue is not triggered
    participant3.ledger_api.javaapi.commands.submit(
      Seq(participant3.id.adminParty),
      new Iou(
        participant3.id.adminParty.toProtoPrimitive,
        alice.toProtoPrimitive,
        new Amount(17.toBigDecimal, "CC"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq,
      commandId = "commandId",
    )

    val mediator2Identity = mediator2.topology.transactions.identity_transactions()
    sequencer1.topology.transactions.load(
      mediator2Identity,
      store = synchronizer1Id,
      ForceFlag.AlienMember,
    )
    eventually() {
      sequencer1.topology.transactions
        .list(
          store = synchronizer1Id,
          filterNamespace = mediator2.namespace.filterString,
          filterMappings =
            Seq(TopologyMapping.Code.NamespaceDelegation, TopologyMapping.Code.OwnerToKeyMapping),
        )
        .result
        .map(_.mapping.code) should have size 2
    }

    sequencer1.topology.mediators.propose(
      synchronizer1Id,
      threshold = PositiveInt.one,
      active = Seq(mediator1.id, mediator2.id),
      group = NonNegativeInt.zero,
      synchronize = None,
    )

    // Topology txs (proposals) to trigger the bug in the topology caching,
    // has to be sequenced between the mediator state sequencing and effective time
    (1 to 3).foreach { i =>
      val participant = lp(s"participant$i")
      participant.topology.party_to_participant_mappings.propose(
        // NB: creates proposals as participant1 is not authorized for sequencer1 namespace
        PartyId.tryCreate(s"party_$i", sequencer1.namespace),
        newParticipants = Seq(
          (
            participant.id,
            ParticipantPermission.Observation,
          )
        ),
        synchronize = None,
      )
    }

    // A confirmation traffic reaching the new mediator after it's mediator state becoming effective,
    // but before the above topology tx proposals effective time
    (2 to 10).foreach { i =>
      val participant = lp(s"participant${(i % 3) + 1}")
      participant.ledger_api.javaapi.commands.submit_async(
        Seq(participant.id.adminParty),
        new Iou(
          participant.id.adminParty.toProtoPrimitive,
          alice.toProtoPrimitive,
          new Amount(i.toBigDecimal, "CC"),
          List.empty.asJava,
        ).create.commands.asScala.toSeq,
        commandId = s"commandId$i",
      )
    }

    eventually() {
      sequencer1.topology.mediators
        .list(synchronizer1Id)
        .loneElement
        .item
        .active
        .forgetNE should contain(mediator2.id)
    }

    mediator2.setup.assign(
      synchronizer1Id,
      SequencerConnections.single(sequencer1.sequencerConnection),
    )
    mediator2.health.wait_for_initialized()

    participant3.health.ping(participant1)
  }
}

class MediatorOnboardingConcurrentTrafficTestPostgres
    extends MediatorOnboardingConcurrentTrafficTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
