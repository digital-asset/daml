// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  PartiesAllocator,
  PartyToParticipantDeclarative,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}

final class PartyToParticipantDeclarativeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, daName)
      participants.all.synchronizers.connect_local(sequencer2, acmeName)
      participants.all.dars.upload(CantonExamplesPath)
    }

  "Party to participant declarative" should {
    "support all topology changes" in { implicit env =>
      import env.*

      // We consider only one synchronizer in this case since changes are per synchronizer

      val chopper = participant1.parties.enable("chopper", synchronizer = daName)
      participant1.parties.enable("chopper", synchronizer = acmeName)

      def changeTopology(
          newThreshold: PositiveInt,
          newPermissions: Set[(ParticipantId, ParticipantPermission)],
      ): Unit = PartyToParticipantDeclarative(Set(participant1, participant2), Set(daId))(
        owningParticipants = Map(chopper -> participant1),
        targetTopology = Map(chopper -> Map(daId -> (newThreshold, newPermissions))),
      )

      // replication on p2
      changeTopology(
        PositiveInt.one,
        Set((participant1, Submission), (participant2, Submission)),
      )

      // changing threshold from one to two and permission on p2
      changeTopology(
        PositiveInt.two,
        Set((participant1, Submission), (participant2, Confirmation)),
      )

      // offboarding from p1 and changing permission on p2
      changeTopology(
        PositiveInt.one,
        Set((participant2, Submission)),
      )

      // onboarding to p1 again
      changeTopology(
        PositiveInt.one,
        Set((participant1, Confirmation), (participant2, Submission)),
      )
    }

    "allow party offboarding" in { implicit env =>
      import env.*

      val bob = participant1.parties.enable("bob", synchronizer = daName)
      participant1.parties.enable("bob", synchronizer = acmeName)

      Seq(daId, acmeId).foreach { synchronizerId =>
        PartyToParticipantDeclarative.forParty(Set(participant1), synchronizerId)(
          participant1,
          bob,
          PositiveInt.one,
          Set((participant1, Submission)),
        )
      }
    }
  }

  "Parties allocator" should {
    "work in complex scenarios" in { implicit env =>
      import env.*

      /*
      Interesting features:
      - signatory is owned by P1 but not hosted on P1 on acme
      - decentralized has threshold 2
      - various permissions
      - second observer is owned by P1 but only hosted with observations rights on P1
       */

      PartiesAllocator(Set(participant1, participant2))(
        newParties = Seq(
          "signatory" -> participant1.id,
          "observer" -> participant2.id,
          "decentralized" -> participant1.id,
          "second observer" -> participant1.id,
        ),
        targetTopology = Map(
          "signatory" -> Map(
            daId -> (PositiveInt.one, Set(
              (participant1, Submission),
              (participant2, Observation),
            )),
            acmeId -> (PositiveInt.one, Set((participant2, Submission))),
          ),
          "observer" -> Map(
            daId -> (PositiveInt.one, Set((participant2, Observation))),
            acmeId -> (PositiveInt.one, Set((participant2, Observation))),
          ),
          "decentralized" -> Map(
            daId -> (PositiveInt.two, Set(
              (participant1, Submission),
              (participant2, Confirmation),
            ))
          ),
          "second observer" -> Map(
            acmeId -> (PositiveInt.one, Set((participant1, Observation)))
          ),
        ),
      )

      val signatory = "signatory".toPartyId()
      val observer = "observer".toPartyId()

      IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
    }
  }
}
