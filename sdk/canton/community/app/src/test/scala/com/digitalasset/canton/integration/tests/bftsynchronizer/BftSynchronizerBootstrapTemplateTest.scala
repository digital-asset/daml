// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion

sealed trait BftSynchronizerBootstrapTemplateTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2

  "BFT Synchronizer" when {

    "Basic synchronizer startup" onlyRunWithOrGreaterThan ProtocolVersion.dev in { implicit env =>
      import env.*

      // STEP 1: connect participants for the synchronizer via "their" sequencers
      clue("participant1 connects to sequencer1, sequencer2") {
        participant1.synchronizers.connect_local_bft(
          NonEmpty
            .mk(
              Seq,
              SequencerAlias.tryCreate("seq1x") -> sequencer1,
              SequencerAlias.tryCreate("seq2x") -> sequencer2,
            )
            .toMap,
          alias = daName,
        )
      }
      clue("participant2 connects to sequencer1, sequencer2") {
        participant2.synchronizers.connect_local_bft(
          NonEmpty
            .mk(
              Seq,
              SequencerAlias.tryCreate("seq2x") -> sequencer2,
              SequencerAlias.tryCreate("seq1x") -> sequencer1,
            )
            .toMap,
          alias = daName,
        )
      }
      clue("participant3 connects to sequencer1") {
        participant3.synchronizers.connect_local(sequencer1, daName)
      }

      // STEP 2: participants can now transact with each other
      participant1.health.ping(participant2.id)

      // STEP 3: allocate a plain-old-party
      val testPartyP1 =
        participant1.parties.enable(
          "test-party-p1",
          synchronizeParticipants = Seq(participant2),
        )
      val testPartyP2 =
        participant2.parties.enable(
          "test-party-p2",
          synchronizeParticipants = Seq(participant1),
        )
      checkPartiesExist(testPartyP1, testPartyP2)
    }
  }

  private def checkPartiesExist(
      parties: PartyId*
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val knownParties = Set(
      participant1.id.adminParty,
      participant2.id.adminParty,
      participant3.id.adminParty,
    ) ++ parties

    eventually() {
      val p1Parties = participant1.parties.list()
      p1Parties.map(_.party) should contain theSameElementsAs (knownParties)
    }

    eventually() {
      val p2Parties = participant2.parties.list()
      p2Parties.map(_.party) should contain theSameElementsAs (knownParties)
    }
  }
}

class BftSynchronizerBootstrapTemplateTestDefault extends BftSynchronizerBootstrapTemplateTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory
    )
  )
}

//class BftSynchronizerBootstrapTemplateTestPostgres extends BftSynchronizerBootstrapTemplateTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}
